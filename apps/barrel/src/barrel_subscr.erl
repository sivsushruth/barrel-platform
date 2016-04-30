%%%-------------------------------------------------------------------
%%% @author benoitc
%%% @copyright (C) 2016, Enki Multimedia
%%% @doc
%%%
%%% @end
%%% Created : 30. Apr 2016 11:09
%%%-------------------------------------------------------------------
-module(barrel_subscr).
-author("benoitc").

%% API
-export([subscribe/1]).
-export([report_db_event/2]).
-export([report_log_event/1]).
-export([report_index_event/3]).

-include_lib("stdlib/include/ms_transform.hrl").

subscribe(activity) ->
  change_subscr(activity);
subscribe({db_update, DbName}) ->
  change_subscr({db_update, DbName});
subscribe({db_update, DbName, doc}) ->
  change_subscr({db_update, DbName, doc});
subscribe({db_update, DbName, ddoc}) ->
  change_subscr({db_update, DbName, ddoc});
subscribe({index_update, DbName, IndexName}) ->
  change_subscr({index_update, DbName, IndexName});
subscribe(log) ->
  change_subscr(log);
subscribe(What) ->
  {error, {badarg, What}}.

change_subscr(What) ->
  case What of
    {db_update, DbName} ->
      gproc:reg({p, l, {DbName, subscribers}});
    {db_update, DbName, Type} ->
      Cond = case Type of
               doc ->
                 [{{db_update, DbName, updated}, [], [true]}];
               ddoc ->
                 [{{db_update, DbName, {ddoc_updated, '_'}}, [], [true]}]
             end,
      gproc:reg({p, l, {DbName, subscribers}}, Cond);
    {index_update, DbName, IndexName} ->
      gproc:reg({p, l, {{DbName, IndexName}, subscribers}});
    log ->
      gproc:reg({p, l, {barrel_log, subscribers}});
    activity ->
      gproc:reg({p, l, barrel_activity})
  end.


report_db_event(DbName, What) ->
  ActivitySubs =  gproc:select({l,p}, [{{{p, l, barrel_activity}, '$1', '_'}, [], ['$1'] }]),
  DbSubs =  gproc:select({l,p}, [{{{p, l, {DbName, subscribers}}, '$1', '$2'}, [], [{{'$1', '$2'}}] }]),

  Msg = case What of
          created -> {db_update, DbName, created};
          updated -> {db_update, DbName, updated};
          deleted -> {db_update, DbName, deleted};
          {ddoc_updated, DDoc} -> {db_update, DbName, {ddoc_updated, DDoc}};
          _ -> erlang:error(badarg)
        end,

  deliver(ActivitySubs, Msg),
  deliver_with_cond(DbSubs, Msg).


report_log_event(LogEvent) ->
  LogSubs =  gproc:select({l,p}, [{{{p, l, barrel_log}, '$1', '_'}, [], ['$1'] }]),
  deliver(LogSubs, LogEvent).

report_index_event(DbName, IndexName, IndexEvent) ->
  ActivitySubs =  gproc:select({l,p}, [{{{p, l, barrel_activity}, '$1', '_'}, [], ['$1'] }]),
  IndexSubs =  gproc:select({l,p}, [{{{p, l, DbName, IndexName}, '$1', '_'}, [], ['$1'] }]),

  deliver(ActivitySubs, IndexEvent),
  deliver(IndexSubs, IndexEvent).


deliver([], _Msg) ->
  ok;
deliver([Pid | Rest], Msg) ->
  Pid ! Msg,
  deliver(Rest, Msg).

deliver_with_cond([], _Msg) ->
  ok;
deliver_with_cond([{Pid, Cond} | Rest], Msg) ->
  case Cond of
    undefined ->
      Pid ! Msg;
    _ ->
      try C = ets:match_spec_compile(Cond),
        case ets:match_spec_run([Msg, C]) of
          [true] -> Pid ! Msg;
          _Else -> ok
        end
      catch
        error:_ -> ok
      end
  end,
  deliver_with_cond(Rest, Msg).