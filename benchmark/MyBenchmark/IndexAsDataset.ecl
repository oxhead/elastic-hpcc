//
//  Example code - use without restriction.  
//
IMPORT $;
IMPORT Std;

OutRec := RECORD
  INTEGER   Seq;
  QSTRING15 FirstName;
  QSTRING25 LastName;
  STRING2   State;
END;

IDX  := $.DeclareData.IDX__Person_LastName_FirstName_Payload;
Base := $.DeclareData.Person.File;

OutRec XF1(IDX L, INTEGER C) := TRANSFORM
  SELF.Seq := C;
  SELF := L;
END;

O1 := PROJECT(IDX(KEYED(LastName='ABBENANTE')),
							XF1(LEFT,COUNTER));

//O1 := PROJECT(IDX(KEYED(lastname='ABBENANTE'),
//                  KEYED(firstname='AALIYAH')),
//							XF1(LEFT,COUNTER));
OUTPUT(O1,ALL);

OutRec XF2(Base L, INTEGER C) := TRANSFORM
  SELF.Seq := C;
  SELF := L;
END;

O2 := PROJECT(Base(lastname='ABBENANTE',
                   firstname='AALIYAH'),
							XF2(LEFT,COUNTER));
OUTPUT(O2,ALL);
