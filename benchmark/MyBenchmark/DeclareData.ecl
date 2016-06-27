//
//  Example code - use without restriction.  
//
IMPORT Std;

EXPORT DeclareData := MODULE

	EXPORT OUTPUT_DIR := '~MyBenchmark::';
	EXPORT LZ_IP  := '10.173.9.4';		//This MUST be changed to the Landing Zone IP for your configuration
	EXPORT LZ_Dir := 'ProgGuide/';

	
	EXPORT Layout_Person_Account_Simple := RECORD
		UNSIGNED3 PersonID;
		STRING15 FirstName;
		STRING25 LastName;
		STRING20 Account;
		UNSIGNED4 Balance;
	END;
	EXPORT Layout_Person_Simple := RECORD
		UNSIGNED3 PersonID;
		STRING15 FirstName;
		STRING25 LastName;
	END;
	EXPORT Layout_Person := RECORD
		UNSIGNED3 PersonID;
		STRING15 FirstName;
		STRING25 LastName;
		STRING1   MiddleInitial;
		STRING1   Gender;
		STRING42 Street;
		STRING20 City;
		STRING2   State;
		STRING5  Zip;
	END;
	EXPORT Layout_Accounts := RECORD
		STRING20 Account;
		STRING8  OpenDate;
		STRING2   IndustryCode;
		STRING1   AcctType;
		STRING1   AcctRate;
		UNSIGNED1 Code1;
		UNSIGNED1 Code2;
		UNSIGNED4 HighCredit;
		UNSIGNED4 Balance;
	END;
	EXPORT Layout_Accounts_Link := RECORD
		UNSIGNED3 PersonID;
		Layout_Accounts;
	END;
	SHARED Layout_Combined := RECORD
		Layout_Person;
		DATASET(Layout_Accounts) Accounts;
	END;

  EXPORT Person_Dist := MODULE
	  EXPORT DATA_PATH := OUTPUT_DIR+'Dist::Data_People_';
		EXPORT KEY_PATH := OUTPUT_DIR+'Dist::Idx_People_';
		EXPORT FilePlus_PersonID := DATASET(DATA_PATH+'PersonID',{Layout_Person,UNSIGNED8 RecPos{virtual(fileposition)}}, THOR);
		EXPORT FilePlus_FirstName := DATASET(DATA_PATH+'FirstName',{Layout_Person,UNSIGNED8 RecPos{virtual(fileposition)}}, THOR);
	  EXPORT FilePlus_LastName := DATASET(DATA_PATH+'LastName',{Layout_Person,UNSIGNED8 RecPos{virtual(fileposition)}}, THOR);
		EXPORT FilePlus_State := DATASET(DATA_PATH+'State',{Layout_Person,UNSIGNED8 RecPos{virtual(fileposition)}}, THOR);
	  EXPORT FilePlus_City := DATASET(DATA_PATH+'City',{Layout_Person,UNSIGNED8 RecPos{virtual(fileposition)}}, THOR);
		EXPORT FilePlus_Zip := DATASET(DATA_PATH+'Zip',{Layout_Person,UNSIGNED8 RecPos{virtual(fileposition)}}, THOR);
    EXPORT IDX_PersonID := INDEX(FilePlus_PersonID,{PersonID,RecPos},KEY_PATH+'PersonID');
	  EXPORT IDX_FirstName := INDEX(FilePlus_FirstName,{FirstName,RecPos},KEY_PATH+'FirstName');
		EXPORT IDX_LastName := INDEX(FilePlus_LastName,{LastName,RecPos},KEY_PATH+'LastName');
		EXPORT IDX_State := INDEX(FilePlus_State,{State,RecPos},KEY_PATH+'State');
		EXPORT IDX_City := INDEX(FilePlus_City,{City,RecPos},KEY_PATH+'City');
		EXPORT IDX_Zip := INDEX(FilePlus_Zip,{Zip,RecPos},KEY_PATH+'Zip');	
	END;	
	
	EXPORT Person_Sort := MODULE
	  EXPORT DATA_PATH := OUTPUT_DIR+'Sort::Data_People_';
		EXPORT KEY_PATH := OUTPUT_DIR+'Sort::Idx_People_';
		EXPORT FilePlus_PersonID := DATASET(DATA_PATH+'PersonID',{Layout_Person,UNSIGNED8 RecPos{virtual(fileposition)}}, THOR);
		EXPORT FilePlus_FirstName := DATASET(DATA_PATH+'FirstName',{Layout_Person,UNSIGNED8 RecPos{virtual(fileposition)}}, THOR);
	  EXPORT FilePlus_LastName := DATASET(DATA_PATH+'LastName',{Layout_Person,UNSIGNED8 RecPos{virtual(fileposition)}}, THOR);
		EXPORT FilePlus_State := DATASET(DATA_PATH+'State',{Layout_Person,UNSIGNED8 RecPos{virtual(fileposition)}}, THOR);
	  EXPORT FilePlus_City := DATASET(DATA_PATH+'City',{Layout_Person,UNSIGNED8 RecPos{virtual(fileposition)}}, THOR);
		EXPORT FilePlus_Zip := DATASET(DATA_PATH+'Zip',{Layout_Person,UNSIGNED8 RecPos{virtual(fileposition)}}, THOR);
    EXPORT IDX_PersonID := INDEX(FilePlus_PersonID,{PersonID,RecPos},KEY_PATH+'PersonID');
	  EXPORT IDX_FirstName := INDEX(FilePlus_FirstName,{FirstName,RecPos},KEY_PATH+'FirstName');
		EXPORT IDX_LastName := INDEX(FilePlus_LastName,{LastName,RecPos},KEY_PATH+'LastName');
		EXPORT IDX_State := INDEX(FilePlus_State,{State,RecPos},KEY_PATH+'State');
		EXPORT IDX_City := INDEX(FilePlus_City,{City,RecPos},KEY_PATH+'City');
		EXPORT IDX_Zip := INDEX(FilePlus_Zip,{Zip,RecPos},KEY_PATH+'Zip');	
	END;
	
	EXPORT Accounts_Dist := MODULE
		EXPORT FilePlus_PersonID := DATASET(OUTPUT_DIR+'Dist::Data_Accounts_PersonID',{Layout_Accounts_Link,UNSIGNED8 RecPos{virtual(fileposition)}}, THOR);
    EXPORT IDX_PersonID := INDEX(FilePlus_PersonID,{PersonID,RecPos},OUTPUT_DIR+'Dist::Idx_Accounts_PersonID');
	END;
	
	EXPORT Accounts_Sort := MODULE
    EXPORT FilePlus_PersonID := DATASET(OUTPUT_DIR+'Sort::Data_Accounts_PersonID',{Layout_Accounts_Link,UNSIGNED8 RecPos{virtual(fileposition)}}, THOR);
    EXPORT IDX_PersonID := INDEX(FilePlus_PersonID,{PersonID,RecPos},OUTPUT_DIR+'Sort::Idx_Accounts_PersonID');	
	END;
END;
