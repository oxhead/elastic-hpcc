IMPORT TutorialYourName;
EXPORT File_TutorialPerson :=
DATASET('~tutorial::YN::TutorialPerson',
 {TutorialYourName.Layout_People,
 UNSIGNED8 fpos {virtual(fileposition)}},THOR);
