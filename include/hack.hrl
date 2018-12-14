
-define(hack_tag(Tag), Tag =:= noop orelse
                       Tag =:= ping orelse
                       Tag =:= timed_read).
