IMPORT TutorialYourName;
ZipFilter :='33024';
FetchPeopleByZip :=
FETCH(TutorialYourName.File_TutorialPerson,
 TutorialYourName.IDX_PeopleByZIP(zip=ZipFilter),
 RIGHT.fpos);
OUTPUT(FetchPeopleByZip);
