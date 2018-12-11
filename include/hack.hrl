
-define(hack_tag(Tag), Tag =:= ping orelse
                       Tag =:= ntping orelse
                       Tag =:= ntpread).
