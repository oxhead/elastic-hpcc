IMPORT Std;
layout_word_list := record
 string word;
end;
File_Word_List := dataset('~thor::word_list_csv', layout_word_list,
 CSV(heading(1),separator(','),quote('')));
STRING Word := 'teacher' :STORED('Word');
STRING SortString(STRING input) := FUNCTION
 OneChar := RECORD
 STRING c;
 END;
 OneChar MakeSingle(OneChar L, unsigned pos) := TRANSFORM
 SELF.c := L.c[pos];
 END;
 Split := NORMALIZE(DATASET([input],OneChar), LENGTH(input),
 MakeSingle(LEFT,COUNTER));
 SortedSplit := SORT(Split, c);
 OneChar Recombine(OneChar L, OneChar R) := TRANSFORM
 SELF.c := L.c+R.c;
 END;
 Recombined := ROLLUP(SortedSplit, Recombine(LEFT, RIGHT),ALL);
 RETURN Recombined[1].c;
END;
STRING CleanedWord := SortString(TRIM(Std.Str.ToUpperCase(Word)));
R := RECORD
 STRING SoFar {MAXLENGTH(200)};
 STRING Rest {MAXLENGTH(200)};
END;
Init := DATASET([{'',CleanedWord}],R);
R Pluck1(DATASET(R) infile) := FUNCTION
 R TakeOne(R le, UNSIGNED1 c) := TRANSFORM
 SELF.SoFar := le.SoFar + le.Rest[c];
 SELF.Rest := le.Rest[..c-1]+le.Rest[c+1..];
 // Boundary Conditions
 // handled automatically
 END;
 RETURN DEDUP(NORMALIZE(infile,LENGTH(LEFT.Rest),TakeOne(LEFT,COUNTER)));
END;
L := LOOP(Init,LENGTH(CleanedWord),Pluck1(ROWS(LEFT)));
ValidWords := JOIN(L,File_Word_List,
LEFT.SoFar=Std.Str.ToUpperCase(RIGHT.Word),TRANSFORM(LEFT));
OUTPUT(CleanedWord);
COUNT(ValidWords);
OUTPUT(ValidWords)
