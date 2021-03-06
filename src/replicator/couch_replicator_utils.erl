%% Copyright 2016, Benoit Chesneau
%% Copyright 2009-2014 The Apache Software Foundation
%%
% Licensed under the Apache License, Version 2.0 (the "License"); you may not
% use this file except in compliance with the License. You may obtain a copy of
% the License at
%
%   http://www.apache.org/licenses/LICENSE-2.0
%
% Unless required by applicable law or agreed to in writing, software
% distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
% WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
% License for the specific language governing permissions and limitations under
% the License.

-module(couch_replicator_utils).

-export([parse_rep_doc/2]).
-export([open_db/1, close_db/1]).
-export([start_db_compaction_notifier/2, stop_db_compaction_notifier/1]).
-export([replication_id/2]).
-export([sum_stats/2]).


%% internal

-export([init_compaction_notifier/2]).

-include("db.hrl").
-include("couch_replicator_api_wrap.hrl").
-include("couch_replicator.hrl").
-include_lib("ibrowse/include/ibrowse.hrl").


parse_rep_doc(Props, UserCtx) ->
  ProxyParams = parse_proxy_params(maps:get(<<"proxy">>, Props, <<>>)),
  Options = make_options(Props),
  case proplists:get_value(cancel, Options, false) andalso
       (proplists:get_value(id, Options, nil) =/= nil) of
    true ->
      {ok, #rep{options = Options, user_ctx = UserCtx}};
    false ->
      Source = parse_rep_db(maps:get(<<"source">>, Props),
                            ProxyParams, Options),
      Target = parse_rep_db(maps:get(<<"target">>, Props),
                            ProxyParams, Options),


      {RepType, View} = case catch maps:get(<<"filter">>, Props) of
                          <<"_view">> ->
                            QP  = proplists:get_value(query_params, Options, #{}),
                            ViewParam = maps:get(<<"view">>, QP),
                            View1 = case binary:split(ViewParam, <<"/">>) of
                                      [DName, ViewName] ->
                                        {<< "_design/", DName/binary >>, ViewName};
                                      _ ->
                                        throw({bad_request, "Invalid `view` parameter."})
                                    end,
                            {view, View1};
                          _ ->
                            {db, nil}
                        end,

      Rep = #rep{
               source = Source,
               target = Target,
               options = Options,
               user_ctx = UserCtx,
               type = RepType,
               view = View,
               doc_id = maps:get(<<"_id">>, Props, null)
              },
      {ok, Rep#rep{id = replication_id(Rep)}}
  end.


replication_id(#rep{options = Options} = Rep) ->
  BaseId = replication_id(Rep, ?REP_ID_VERSION),
  {BaseId, list_to_binary(maybe_append_options([continuous, create_target], Options))}.


% Versioned clauses for generating replication IDs.
% If a change is made to how replications are identified,
% please add a new clause and increase ?REP_ID_VERSION.

replication_id(#rep{user_ctx = UserCtx} = Rep, 1) ->
    UUID = barrel_server:node_id(),
    Src = get_rep_endpoint(UserCtx, Rep#rep.source),
    Tgt = get_rep_endpoint(UserCtx, Rep#rep.target),
    maybe_append_filters([UUID, Src, Tgt], Rep).


maybe_append_filters(Base,
        #rep{source = Source, user_ctx = UserCtx, options = Options}) ->
    Base2 = Base ++
        case proplists:get_value(filter, Options) of
        undefined ->
            case proplists:get_value(doc_ids, Options) of
            undefined ->
                [];
            DocIds ->
                [DocIds]
            end;
        <<"_", _/binary>> = Filter ->
                [Filter, proplists:get_value(query_params, Options, #{})];
        Filter ->
            [filter_code(Filter, Source, UserCtx),
                proplists:get_value(query_params, Options, #{})]
        end,
    barrel_lib:to_hex(crypto:hash(md5, term_to_binary(Base2))).


filter_code(Filter, Source, UserCtx) ->
    {DDocName, FilterName} =
    case re:run(Filter, "(.*?)/(.*)", [{capture, [1, 2], binary}]) of
    {match, [DDocName0, FilterName0]} ->
        {DDocName0, FilterName0};
    _ ->
        throw({error, <<"Invalid filter. Must match `ddocname/filtername`.">>})
    end,
    Db = case (catch couch_replicator_api_wrap:db_open(Source, [{user_ctx, UserCtx}])) of
    {ok, Db0} ->
        Db0;
    DbError ->
        DbErrorMsg = io_lib:format("Could not open source database `~s`: ~s",
           [couch_replicator_api_wrap:db_uri(Source), barrel_lib:to_binary(DbError)]),
        throw({error, iolist_to_binary(DbErrorMsg)})
    end,
    try
        Body = case (catch couch_replicator_api_wrap:open_doc(
            Db, <<"_design/", DDocName/binary>>, [ejson_body])) of
        {ok, #doc{body = Body0}} ->
            Body0;
        DocError ->
            DocErrorMsg = io_lib:format(
                "Couldn't open document `_design/~s` from source "
                "database `~s`: ~s", [DDocName, couch_replicator_api_wrap:db_uri(Source),
                    barrel_lib:to_binary(DocError)]),
            throw({error, iolist_to_binary(DocErrorMsg)})
        end,
        Code = barrel_lib:get_nested_json_value(
            Body, [<<"filters">>, FilterName]),
        re:replace(Code, [$^, "\s*(.*?)\s*", $$], "\\1", [{return, binary}])
    after
        couch_replicator_api_wrap:db_close(Db)
    end.


maybe_append_options(Options, RepOptions) ->
    lists:foldl(fun(Option, Acc) ->
        Acc ++
        case proplists:get_value(Option, RepOptions, false) of
        true ->
            "+" ++ atom_to_list(Option);
        false ->
            ""
        end
    end, [], Options).


get_rep_endpoint(_UserCtx, #httpdb{url=Url, headers=Headers, oauth=OAuth}) ->
    DefaultHeaders = (#httpdb{})#httpdb.headers,
    case OAuth of
    nil ->
        {remote, Url, Headers -- DefaultHeaders};
    #oauth{} ->
        {remote, Url, Headers -- DefaultHeaders, OAuth}
    end;
get_rep_endpoint(UserCtx, <<DbName/binary>>) ->
    {local, DbName, UserCtx}.


parse_rep_db(Props, ProxyParams, Options) when is_map(Props) ->
    Url = maybe_add_trailing_slash(maps:get(<<"url">>, Props)),
    AuthProps = maps:get(<<"auth">>, Props, #{}),
    BinHeaders = maps:get(<<"headers">>, Props, #{}),
    Headers = lists:ukeysort(1, [{binary_to_list(K), ?b2l(V)} || {K, V} <- maps:to_list(BinHeaders)]),
    DefaultHeaders = (#httpdb{})#httpdb.headers,
    OAuth = case maps:get(<<"oauth">>, AuthProps, undefined) of
    undefined ->
        nil;
    OauthProps ->
        #oauth{
            consumer_key = binary_to_list(maps:get(<<"consumer_key">>, OauthProps)),
            token = binary_to_list(maps:get(<<"token">>, OauthProps)),
            token_secret = binary_to_list(maps:get(<<"token_secret">>, OauthProps)),
            consumer_secret = binary_to_list(maps:get(<<"consumer_secret">>, OauthProps)),
            signature_method =
                case maps:get(<<"signature_method">>, OauthProps, undefined) of
                undefined ->        hmac_sha1;
                <<"PLAINTEXT">> ->  plaintext;
                <<"HMAC-SHA1">> ->  hmac_sha1;
                <<"RSA-SHA1">> ->   rsa_sha1
                end
        }
    end,
    #httpdb{
        url = Url,
        oauth = OAuth,
        headers = lists:ukeymerge(1, Headers, DefaultHeaders),
        ibrowse_options = lists:keysort(1,
            [{socket_options, proplists:get_value(socket_options, Options)} |
                ProxyParams ++ ssl_params(Url)]),
        timeout = proplists:get_value(connection_timeout, Options),
        http_connections = proplists:get_value(http_connections, Options),
        retries = proplists:get_value(retries, Options)
    };
parse_rep_db(<<"http://", _/binary>> = Url, ProxyParams, Options) ->
    parse_rep_db(#{<<"url">> => Url}, ProxyParams, Options);
parse_rep_db(<<"https://", _/binary>> = Url, ProxyParams, Options) ->
    parse_rep_db(#{<<"url">> => Url}, ProxyParams, Options);
parse_rep_db(<<DbName/binary>>, _ProxyParams, _Options) ->
    DbName.


maybe_add_trailing_slash(Url) when is_binary(Url) ->
    maybe_add_trailing_slash(binary_to_list(Url));
maybe_add_trailing_slash(Url) ->
    case lists:last(Url) of
    $/ ->
        Url;
    _ ->
        Url ++ "/"
    end.


make_options(Props) ->
    Options = lists:ukeysort(1, convert_options(maps:to_list(Props))),
    DefWorkers = barrel_config:get_env(worker_processes),
    DefBatchSize = barrel_config:get_env(worker_batch_size),
    DefConns = barrel_config:get_env(http_connections),
    DefTimeout = barrel_config:get_env(connection_timeout),
    DefRetries = barrel_config:get_env(retries_per_request),
    UseCheckpoints =  barrel_config:get_env(use_checkpoints),
    DefCheckpointInterval = barrel_config:get_env(checkpoint_interval),
    DefSocketOptions = barrel_config:get_env(socket_options),
    lists:ukeymerge(1, Options, lists:keysort(1, [
        {connection_timeout, DefTimeout},
        {retries, DefRetries},
        {http_connections, DefConns},
        {socket_options, DefSocketOptions},
        {worker_batch_size, DefBatchSize},
        {worker_processes, DefWorkers},
        {use_checkpoints, UseCheckpoints},
        {checkpoint_interval, DefCheckpointInterval}
    ])).


convert_options([])->
    [];
convert_options([{<<"cancel">>, V} | R]) ->
    [{cancel, V} | convert_options(R)];
convert_options([{IdOpt, V} | R]) when IdOpt =:= <<"_local_id">>;
        IdOpt =:= <<"replication_id">>; IdOpt =:= <<"id">> ->
    Id = lists:splitwith(fun(X) -> X =/= $+ end, binary_to_list(V)),
    [{id, Id} | convert_options(R)];
convert_options([{<<"create_target">>, V} | R]) ->
    [{create_target, V} | convert_options(R)];
convert_options([{<<"continuous">>, V} | R]) ->
    [{continuous, V} | convert_options(R)];
convert_options([{<<"filter">>, V} | R]) ->
    [{filter, V} | convert_options(R)];
convert_options([{<<"query_params">>, V} | R]) ->
    [{query_params, V} | convert_options(R)];
convert_options([{<<"doc_ids">>, null} | R]) ->
    convert_options(R);
convert_options([{<<"doc_ids">>, V} | R]) ->
    % Ensure same behaviour as old replicator: accept a list of percent
    % encoded doc IDs.
    DocIds = [list_to_binary(couch_httpd:unquote(Id)) || Id <- V],
    [{doc_ids, DocIds} | convert_options(R)];
convert_options([{<<"worker_processes">>, V} | R]) ->
    [{worker_processes, barrel_lib:to_integer(V)} | convert_options(R)];
convert_options([{<<"worker_batch_size">>, V} | R]) ->
    [{worker_batch_size, barrel_lib:to_integer(V)} | convert_options(R)];
convert_options([{<<"http_connections">>, V} | R]) ->
    [{http_connections, barrel_lib:to_integer(V)} | convert_options(R)];
convert_options([{<<"connection_timeout">>, V} | R]) ->
    [{connection_timeout, barrel_lib:to_integer(V)} | convert_options(R)];
convert_options([{<<"retries_per_request">>, V} | R]) ->
    [{retries, barrel_lib:to_integer(V)} | convert_options(R)];
convert_options([{<<"socket_options">>, V} | R]) ->
    {ok, SocketOptions} = barrel_lib:parse_term(V),
    [{socket_options, SocketOptions} | convert_options(R)];
convert_options([{<<"since_seq">>, V} | R]) ->
    [{since_seq, V} | convert_options(R)];
convert_options([{<<"use_checkpoints">>, V} | R]) ->
    [{use_checkpoints, V} | convert_options(R)];
convert_options([{<<"checkpoint_interval">>, V} | R]) ->
    [{checkpoint_interval, barrel_lib:to_integer(V)} | convert_options(R)];
convert_options([_ | R]) -> % skip unknown option
    convert_options(R).


parse_proxy_params(ProxyUrl) when is_binary(ProxyUrl) ->
    parse_proxy_params(binary_to_list(ProxyUrl));
parse_proxy_params([]) ->
    [];
parse_proxy_params(ProxyUrl) ->
    #url{
        host = Host,
        port = Port,
        username = User,
        password = Passwd
    } = ibrowse_lib:parse_url(ProxyUrl),
    [{proxy_host, Host}, {proxy_port, Port}] ++
        case is_list(User) andalso is_list(Passwd) of
        false ->
            [];
        true ->
            [{proxy_user, User}, {proxy_password, Passwd}]
        end.


ssl_params(Url) ->
    case ibrowse_lib:parse_url(Url) of
    #url{protocol = https} ->
        ClientOptions = barrel_config:get_env(replicator_sslopts),
        Depth = proplists:get_value(ssl_certificate_max_depth, ClientOptions, 3),
        VerifyCerts = proplists:get_value(verify_ssl_certificates, ClientOptions, false),
        CertFile = proplists:get_value(cert_file, ClientOptions, nil),
        KeyFile = proplists:get_value(key_file, ClientOptions, nil),
        Password = proplists:get_value(password, ClientOptions, nil),
        SslOpts = [{depth, Depth} | ssl_verify_options(VerifyCerts =:= "true")],
        SslOpts1 = case CertFile /= nil andalso KeyFile /= nil of
            true ->
                case Password of
                    nil ->
                        [{certfile, CertFile}, {keyfile, KeyFile}] ++ SslOpts;
                    _ ->
                        [{certfile, CertFile}, {keyfile, KeyFile},
                            {password, Password}] ++ SslOpts
                end;
            false -> SslOpts
        end,
        [{is_ssl, true}, {ssl_options, SslOpts1}];
    #url{protocol = http} ->
        []
    end.

ssl_verify_options(Value) ->
    ssl_verify_options(Value, erlang:system_info(otp_release)).

ssl_verify_options(true, OTPVersion) when OTPVersion >= "R14" ->
    CAFile = barrel_config:get_env(replicator_cafile),
    [{verify, verify_peer}, {cacertfile, CAFile}];
ssl_verify_options(false, OTPVersion) when OTPVersion >= "R14" ->
    [{verify, verify_none}];
ssl_verify_options(true, _OTPVersion) ->
    CAFile = barrel_config:get_env(replicator_cafile),
    [{verify, 2}, {cacertfile, CAFile}];
ssl_verify_options(false, _OTPVersion) ->
    [{verify, 0}].


open_db(#db{name = Name, user_ctx = UserCtx, options = Options}) ->
    {ok, Db} = couch_db:open(Name, [{user_ctx, UserCtx} | Options]),
    Db;
open_db(HttpDb) ->
    HttpDb.


close_db(#db{} = Db) ->
    couch_db:close(Db);
close_db(_HttpDb) ->
    ok.


start_db_compaction_notifier(#db{name = Db1}, #db{name=Db2}) ->
    listen([Db1, Db2]);
start_db_compaction_notifier(#db{name=DbName}, _) ->
    listen([DbName]);
start_db_compaction_notifier(_, #db{name=DbName}) ->
    listen([DbName]);
start_db_compaction_notifier(_, _) ->
    false.

stop_db_compaction_notifier(false) ->
    ok;
stop_db_compaction_notifier(Pid) when is_pid(Pid) ->
    catch unlink(Pid),
    catch exit(Pid, normal).

listen(Dbs) ->
    Parent = self(),
    spawn_link(?MODULE, init_compaction_notifier, [Parent, Dbs]).

init_compaction_notifier(Parent, Dbs) ->
    barrel_event:mreg(Dbs),
    compaction_notifier_loop(Parent).

compaction_notifier_loop(Parent) ->
    receive
        {'$barrel_event', _, compacted}=Event ->
            Parent ! Event,
            compaction_notifier_loop(Parent);
        {'$barrel_event', _, _} ->
            compaction_notifier_loop(Parent);
        _ ->
            ok
    end.

sum_stats(#rep_stats{} = S1, #rep_stats{} = S2) ->
    #rep_stats{
        missing_checked =
            S1#rep_stats.missing_checked + S2#rep_stats.missing_checked,
        missing_found = S1#rep_stats.missing_found + S2#rep_stats.missing_found,
        docs_read = S1#rep_stats.docs_read + S2#rep_stats.docs_read,
        docs_written = S1#rep_stats.docs_written + S2#rep_stats.docs_written,
        doc_write_failures =
            S1#rep_stats.doc_write_failures + S2#rep_stats.doc_write_failures
    }.
