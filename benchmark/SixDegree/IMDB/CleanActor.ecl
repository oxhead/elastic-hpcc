IMPORT Std;
EXPORT STRING CleanActor(STRING infld) := FUNCTION
 //this can be refined later
 s1 := Std.Str.FindReplace(infld, '\'',''); // replace apostrophe
 s2 := Std.Str.FindReplace(s1, '\t',''); //replace tabs
 s3 := Std.Str.FindReplace(s2, '----',''); // replace multiple -----
 return TRIM(s3, LEFT, RIGHT);
END;
