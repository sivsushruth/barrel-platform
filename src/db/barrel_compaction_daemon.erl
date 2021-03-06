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

-module(barrel_compaction_daemon).
-behaviour(gen_server).

% public API
-export([reg/2, unreg/1]).
-export([pause/1, resume/1]).
-export([tasks/0]).

-export([start_link/0]).

% gen_server callbacks
-export([init/1, handle_call/3, handle_info/2, handle_cast/2]).
-export([code_change/3, terminate/2]).

-include("db.hrl").

-define(CONFIG_ETS, couch_compaction_daemon_config).

-record(state, {
  loop_pid
}).

-record(config, {
  db_frag = nil,
  view_frag = nil,
  period = nil,
  cancel = false,
  parallel_view_compact = false
}).

-record(period, {
  from = nil,
  to = nil
}).

-define(DEFAULT_CHECK_INTERVAL, 300).
-define(DEFAULT_MIN_FILESIZE, 131072).


-type compaction_options() :: [{db_fragmentation, integer()} |
                                {view_fragmentation, integer()} |
                                {from, integer() | string()} |
                                {to, integer() | string()} |
                                {strict_window, boolean()} |
                                {parallel_view_compaction, boolean()}
                                ].

-export_types([compaction_options/0]).

%% @doc register a database to be handled by the compaction daemon
-spec reg(binary(), compaction_options()) -> ok.
reg(DbName, Opts) ->
  case validate_config(Opts, #config{}) of
    {ok, Config}-> gen_server:call(?MODULE, {reg, DbName, Config});
    _ -> erlang:error(badarg)
  end.

%% @doc unregister a database from the compaction daemon.
-spec unreg(binary()) -> ok.
unreg(DbName) ->
  gen_server:call(?MODULE, {unreg, DbName}).

%% @doc pause a compaction task
-spec pause(binary()) -> ok | {error, not_found}.
pause(DbName) ->
  gen_server:call(?MODULE, {pause, DbName}).

%% @doc resume a compaction task previously canceled using `barrel_compaction_daemon:pause/1`
-spec resume(binary()) -> ok | {error, not_found}.
resume(DbName) ->
  gen_server:call(?MODULE, {resume, DbName}).

-spec tasks() -> [{DbName::binary(), compaction_options()}].
tasks() ->
  All = ets:foldl(fun({DbName, Config}, Acc) ->
              [{DbName, config2list(Config)} | Acc]
            end, [], ?CONFIG_ETS),
  lists:usort(All).



config2list(Config) ->
  #config{
    db_frag = DbFrag,
    view_frag = ViewFrag,
    period = Period,
    cancel = Cancel,
    parallel_view_compact = ParralelViewCompact} = Config,

  Opts0 = [{db_frag, DbFrag}, {view_frag, ViewFrag}, {period, Period}, {cancel, Cancel},
    {parallel_view_compact, ParralelViewCompact}],

  Opts1 = lists:filter(fun({_K, nil}) -> false end, Opts0),
  case proplists:get_value(period, Opts1) of
    undefined -> Opts1;
    #period{from=From, to=To} ->
      [{from, From}, {to, To} | proplists:delete(period, Opts1)]
  end.


start_link() ->
  gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

init(_) ->
  process_flag(trap_exit, true),
  ?CONFIG_ETS = ets:new(?CONFIG_ETS, [named_table, set, protected]),
  self() ! init,
  {ok, #state{}}.


handle_cast(_Msg, State) ->
  {noreply, State}.

handle_call({reg, DbName, Config}, _From, State) ->
  true = ets:insert(?CONFIG_ETS, {barrel_lib:to_binary(DbName), Config}),
  {reply, ok, State};

handle_call({unreg, DbName}, _From, State) ->
  ets:delete(?CONFIG_ETS, barrel_lib:to_binary(DbName)),
  {reply, ok, State};

handle_call({pause, DbName}, _From, State) ->
  case ets:lookup(?CONFIG_ETS, DbName) of
    [] -> {reply, {error, not_found}};
    [{DbName, Config}] ->
      ets:insert(?CONFIG_ETS, {barrel_lib:to_binary(DbName), Config#config{cancel=true}}),
      {reply, ok, State}
  end;

handle_call({resume, DbName}, _From, State) ->
  case ets:lookup(?CONFIG_ETS, DbName) of
    [] -> {reply, {error, not_found}};
    [{DbName, Config}] ->
      ets:insert(?CONFIG_ETS, {barrel_lib:to_binary(DbName), Config#config{cancel=false}}),
      {reply, ok, State}
  end;

handle_call(Msg, _From, State) ->
  {stop, {unexpected_call, Msg}, State}.

handle_info(init, State) ->
  load_config(),
  Server = self(),
  Pid = spawn_link(fun() -> compact_loop(Server) end),
  {noreply, State#state{loop_pid=Pid}};

handle_info({'EXIT', Pid, Reason}, #state{loop_pid = Pid} = State) ->
  {stop, {compaction_loop_died, Reason}, State}.


terminate(_Reason, _State) ->
  true = ets:delete(?CONFIG_ETS).


code_change(_OldVsn, State, _Extra) ->
  {ok, State}.


compact_loop(Parent) ->
  {ok, _} = barrel_server:all_databases(
    fun(DbName, Acc) ->
      case ets:info(?CONFIG_ETS, size) =:= 0 of
        true ->
          {stop, Acc};
        false ->
          case get_db_config(DbName) of
            nil ->
              ok;
            {ok, Config} ->
              case check_period(Config) of
                true ->
                  maybe_compact_db(DbName, Config);
                false ->
                  ok
              end
          end,
          {ok, Acc}
      end
    end, ok),
  case ets:info(?CONFIG_ETS, size) =:= 0 of
    true ->
      receive {Parent, have_config} -> ok end;
    false ->
      PausePeriod = barrel_config:get_env(compaction_check_interval),
      ok = timer:sleep(PausePeriod * 1000)
  end,
  compact_loop(Parent).


maybe_compact_db(DbName, Config) ->
  case (catch couch_db:open_int(DbName, [{user_ctx, barrel_lib:adminctx()}])) of
    {ok, Db} ->
      DDocNames = db_ddoc_names(Db),
      case can_db_compact(Config, Db) of
        true ->
          {ok, DbCompactPid} = couch_db:start_compact(Db),
          TimeLeft = compact_time_left(Config),
          {ViewsCompactPid, ViewsMonRef} = case Config#config.parallel_view_compact of
                                             true ->
                                               Pid = spawn_link(fun() ->
                                                 maybe_compact_views(DbName, DDocNames, Config)
                                                                end),
                                               MRef = erlang:monitor(process, Pid),
                                               {Pid, MRef};
                                             false ->
                                               {nil, nil}
                                           end,
          DbMonRef = erlang:monitor(process, DbCompactPid),
          receive
            {'DOWN', DbMonRef, process, _, normal} ->
              couch_db:close(Db),
              case Config#config.parallel_view_compact of
                true ->
                  ok;
                false ->
                  maybe_compact_views(DbName, DDocNames, Config)
              end;
            {'DOWN', DbMonRef, process, _, Reason} ->
              couch_db:close(Db),
              lager:error("Compaction daemon - an error ocurred while"
              " compacting the database `~s`: ~p", [DbName, Reason])
          after TimeLeft ->
            lager:info("Compaction daemon - canceling compaction for database"
            " `~s` because it's exceeding the allowed period.",
              [DbName]),
            erlang:demonitor(DbMonRef, [flush]),
            ok = couch_db:cancel_compact(Db),
            couch_db:close(Db)
          end,
          case ViewsMonRef of
            nil ->
              ok;
            _ ->
              receive
                {'DOWN', ViewsMonRef, process, _, _Reason} ->
                  ok
              after TimeLeft + 1000 ->
                % Under normal circunstances, the view compaction process
                % should have finished already.
                erlang:demonitor(ViewsMonRef, [flush]),
                unlink(ViewsCompactPid),
                exit(ViewsCompactPid, kill)
              end
          end;
        false ->
          couch_db:close(Db),
          maybe_compact_views(DbName, DDocNames, Config)
      end;
    _ ->
      ok
  end.


maybe_compact_views(_DbName, [], _Config) ->
  ok;
maybe_compact_views(DbName, [DDocName | Rest], Config) ->
  case check_period(Config) of
    true ->
      case maybe_compact_view(DbName, DDocName, Config) of
        ok ->
          maybe_compact_views(DbName, Rest, Config);
        timeout ->
          ok
      end;
    false ->
      ok
  end.


db_ddoc_names(Db) ->
  {ok, _, DDocNames} = couch_db:enum_docs(
    Db,
    fun(#full_doc_info{id = <<"_design/", _/binary>>, deleted = true}, _, Acc) ->
      {ok, Acc};
      (#full_doc_info{id = <<"_design/", Id/binary>>}, _, Acc) ->
        {ok, [Id | Acc]};
      (_, _, Acc) ->
        {stop, Acc}
    end, [], [{start_key, <<"_design/">>}, {end_key_gt, <<"_design0">>}]),
  DDocNames.


maybe_compact_view(DbName, GroupId, Config) ->
  DDocId = <<"_design/", GroupId/binary>>,
  case (catch couch_mrview:get_info(DbName, DDocId)) of
    {ok, GroupInfo} ->
      case can_view_compact(Config, DbName, GroupId, GroupInfo) of
        true ->
          {ok, MonRef} = couch_mrview:compact(DbName, DDocId, [monitor]),
          TimeLeft = compact_time_left(Config),
          receive
            {'DOWN', MonRef, process, _, normal} ->
              ok;
            {'DOWN', MonRef, process, _, Reason} ->
              lager:error("Compaction daemon - an error ocurred while compacting"
              " the view group `~s` from database `~s`: ~p",
                [GroupId, DbName, Reason]),
              ok
          after TimeLeft ->
            lager:info("Compaction daemon - canceling the compaction for the "
            "view group `~s` of the database `~s` because it's exceeding"
            " the allowed period.", [GroupId, DbName]),
            erlang:demonitor(MonRef, [flush]),
            ok = couch_mrview:cancel_compaction(DbName, DDocId),
            timeout
          end;
        false ->
          ok
      end;
    Error ->
      lager:error("Error opening view group `~s` from database `~s`: ~p",
        [GroupId, DbName, Error]),
      ok
  end.


compact_time_left(#config{cancel = false}) ->
  infinity;
compact_time_left(#config{period = nil}) ->
  infinity;
compact_time_left(#config{period = #period{to = {ToH, ToM} = To}}) ->
  {H, M, _} = time(),
  case To > {H, M} of
    true ->
      ((ToH - H) * 60 * 60 * 1000) + (abs(ToM - M) * 60 * 1000);
    false ->
      ((24 - H + ToH) * 60 * 60 * 1000) + (abs(ToM - M) * 60 * 1000)
  end.


get_db_config(DbName) ->
  case ets:lookup(?CONFIG_ETS, DbName) of
    [] ->
      case ets:lookup(?CONFIG_ETS, <<"_default">>) of
        [] ->
          nil;
        [{<<"_default">>, Config}] ->
          {ok, Config}
      end;
    [{DbName, Config}] ->
      {ok, Config}
  end.


can_db_compact(#config{db_frag = Threshold} = Config, Db) ->
  case check_period(Config) of
    false ->
      false;
    true ->
      {ok, DbInfo} = couch_db:get_db_info(Db),
      {Frag, SpaceRequired} = frag(DbInfo),
      lager:debug("Fragmentation for database `~s` is ~p%, estimated space for"
      " compaction is ~p bytes.", [Db#db.name, Frag, SpaceRequired]),
      case check_frag(Threshold, Frag) of
        false ->
          false;
        true ->
          Free = free_space(barrel_config:get_env(dir)),
          case Free >= SpaceRequired of
            true ->
              true;
            false ->
              lager:warning("Compaction daemon - skipping database `~s` "
              "compaction: the estimated necessary disk space is about ~p"
              " bytes but the currently available disk space is ~p bytes.",
                [Db#db.name, SpaceRequired, Free]),
              false
          end
      end
  end.

can_view_compact(Config, DbName, GroupId, GroupInfo) ->
  case check_period(Config) of
    false ->
      false;
    true ->
      case proplists:get_value(updater_running, GroupInfo) of
        true ->
          false;
        false ->
          {Frag, SpaceRequired} = frag(GroupInfo),
          lager:debug("Fragmentation for view group `~s` (database `~s`) is "
          "~p%, estimated space for compaction is ~p bytes.",
            [GroupId, DbName, Frag, SpaceRequired]),
          case check_frag(Config#config.view_frag, Frag) of
            false ->
              false;
            true ->
              Free = free_space(couch_index_util:root_dir()),
              case Free >= SpaceRequired of
                true ->
                  true;
                false ->
                  lager:warning("Compaction daemon - skipping view group `~s` "
                  "compaction (database `~s`): the estimated necessary "
                  "disk space is about ~p bytes but the currently available"
                  " disk space is ~p bytes.",
                    [GroupId, DbName, SpaceRequired, Free]),
                  false
              end
          end
      end
  end.


check_period(#config{period = nil}) ->
  true;
check_period(#config{period = #period{from = From, to = To}}) ->
  {HH, MM, _} = erlang:time(),
  case From < To of
    true ->
      ({HH, MM} >= From) andalso ({HH, MM} < To);
    false ->
      ({HH, MM} >= From) orelse ({HH, MM} < To)
  end.


check_frag(nil, _) ->
  true;
check_frag(Threshold, Frag) ->
  Frag >= Threshold.


frag(Info) ->
  FileSize = maps:get(disk_size, Info),
  MinFileSize = barrel_config:get_env(compaction_min_file_size),
  case FileSize < MinFileSize of
    true ->
      {0, FileSize};
    false ->
      case maps:get(data_size, Info) of
        null ->
          {100, FileSize};
        0 ->
          {0, FileSize};
        DataSize ->
          Frag = round(((FileSize - DataSize) / FileSize * 100)),
          {Frag, space_required(DataSize)}
      end
  end.

% Rough, and pessimistic, estimation of necessary disk space to compact a
% database or view index.
space_required(DataSize) ->
  round(DataSize * 2.0).


load_config() ->
  Compactions = barrel_config:get_env(compactions),
  lists:foreach(fun({DbName, Options}) ->
                  case validate_config(Options, #config{}) of
                    {ok, Config}->
                      true = ets:insert(?CONFIG_ETS, {barrel_lib:to_binary(DbName), Config});
                    error ->
                      lager:error("compaction tasks for ~p ignored.~n", [DbName])
                  end
                end, Compactions).

parse_time({HH, MM}) -> {HH, MM};
parse_time(String) ->
  [HH, MM] = string:tokens(String, ":"),
  {list_to_integer(HH), list_to_integer(MM)}.

parse_percent(V) when is_integer(V) -> V;
parse_percent(V) when is_list(V) ->
  case string:tokens(V, "%") of
    [Frag] -> {ok, list_to_integer(Frag)};
    _ -> error
  end;
parse_percent(_) -> error.

validate_config([], #config{period=P} = Config) ->
  if
    P#period.from =:= nil orelse P#period.to =:= nil -> error;
    true -> {ok, Config}
  end;
validate_config([{db_fragmentation, V} | Rest], Config) ->
  {ok, Frag} = parse_percent(V),
  validate_config(Rest, Config#config{db_frag=Frag});
validate_config([{view_fragmentation, V} | Rest], Config) ->
  {ok, Frag} = parse_percent(V),
  validate_config(Rest, Config#config{view_frag=Frag});
validate_config([{from, V} | Rest], Config) ->
  #config{period = P} = Config,
  P2 = case P of
         nil -> #period{from = parse_time(V)};
         _ -> P#period{from = parse_time(V)}
       end,
  validate_config(Rest, Config#config{period=P2});
validate_config([{to, V} | Rest], Config) ->
  #config{period = P} = Config,
  P2 = case P of
         nil -> #period{to = parse_time(V)};
         _ -> P#period{to = parse_time(V)}
       end,
  validate_config(Rest, Config#config{period=P2});
validate_config([{strict_window, true} | Rest], Config) ->
  validate_config(Rest, Config#config{cancel = true});

validate_config([{strict_window, false} | Rest], Config) ->
  validate_config(Rest, Config#config{cancel = false});

validate_config([{parallel_view_compaction, true} | Rest], Config) ->
  validate_config(Rest, Config#config{parallel_view_compact = true});

validate_config([{parallel_view_compaction, false} | Rest], Config) ->
  validate_config(Rest, Config#config{parallel_view_compact = false}).

free_space(Path) ->
  DiskData = lists:sort(
    fun({PathA, _, _}, {PathB, _, _}) ->
      length(filename:split(PathA)) > length(filename:split(PathB))
    end,
    disksup:get_disk_data()),
  free_space_rec(abs_path(Path), DiskData).

free_space_rec(_Path, []) ->
  undefined;
free_space_rec(Path, [{MountPoint0, Total, Usage} | Rest]) ->
  MountPoint = abs_path(MountPoint0),
  case MountPoint =:= string:substr(Path, 1, length(MountPoint)) of
    false ->
      free_space_rec(Path, Rest);
    true ->
      trunc(Total - (Total * (Usage / 100))) * 1024
  end.

abs_path(Path0) ->
  Path = filename:absname(Path0),
  case lists:last(Path) of
    $/ ->
      Path;
    _ ->
      Path ++ "/"
  end.
