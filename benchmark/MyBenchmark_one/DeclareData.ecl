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

    EXPORT Person_Unsorted := MODULE
        EXPORT DATA_PATH := OUTPUT_DIR+'Data_Unsorted_People_';
        EXPORT KEY_PATH := OUTPUT_DIR+'Idx_Unsorted_People_';
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
	
    EXPORT Person_Sorted := MODULE
        EXPORT DATA_PATH := OUTPUT_DIR+'Data_Sorted_People_';
        EXPORT KEY_PATH := OUTPUT_DIR+'Idx_Sorted_People_';
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

        EXPORT FilePlus_FirstName_1 := DATASET(DATA_PATH+'FirstName_1',{Layout_Person,UNSIGNED8 RecPos{virtual(fileposition)}}, THOR);
        EXPORT FilePlus_FirstName_2 := DATASET(DATA_PATH+'FirstName_2',{Layout_Person,UNSIGNED8 RecPos{virtual(fileposition)}}, THOR);
        EXPORT FilePlus_FirstName_3 := DATASET(DATA_PATH+'FirstName_3',{Layout_Person,UNSIGNED8 RecPos{virtual(fileposition)}}, THOR);
        EXPORT FilePlus_FirstName_4 := DATASET(DATA_PATH+'FirstName_4',{Layout_Person,UNSIGNED8 RecPos{virtual(fileposition)}}, THOR);
        EXPORT FilePlus_FirstName_5 := DATASET(DATA_PATH+'FirstName_5',{Layout_Person,UNSIGNED8 RecPos{virtual(fileposition)}}, THOR);
        EXPORT FilePlus_FirstName_6 := DATASET(DATA_PATH+'FirstName_6',{Layout_Person,UNSIGNED8 RecPos{virtual(fileposition)}}, THOR);
        EXPORT FilePlus_FirstName_7 := DATASET(DATA_PATH+'FirstName_7',{Layout_Person,UNSIGNED8 RecPos{virtual(fileposition)}}, THOR);
        EXPORT FilePlus_FirstName_8 := DATASET(DATA_PATH+'FirstName_8',{Layout_Person,UNSIGNED8 RecPos{virtual(fileposition)}}, THOR);
        EXPORT FilePlus_FirstName_9 := DATASET(DATA_PATH+'FirstName_9',{Layout_Person,UNSIGNED8 RecPos{virtual(fileposition)}}, THOR);
        EXPORT FilePlus_FirstName_10 := DATASET(DATA_PATH+'FirstName_10',{Layout_Person,UNSIGNED8 RecPos{virtual(fileposition)}}, THOR);
        EXPORT FilePlus_FirstName_11 := DATASET(DATA_PATH+'FirstName_11',{Layout_Person,UNSIGNED8 RecPos{virtual(fileposition)}}, THOR);
        EXPORT FilePlus_FirstName_12 := DATASET(DATA_PATH+'FirstName_12',{Layout_Person,UNSIGNED8 RecPos{virtual(fileposition)}}, THOR);
        EXPORT FilePlus_FirstName_13 := DATASET(DATA_PATH+'FirstName_13',{Layout_Person,UNSIGNED8 RecPos{virtual(fileposition)}}, THOR);
        EXPORT FilePlus_FirstName_14 := DATASET(DATA_PATH+'FirstName_14',{Layout_Person,UNSIGNED8 RecPos{virtual(fileposition)}}, THOR);
        EXPORT FilePlus_FirstName_15 := DATASET(DATA_PATH+'FirstName_15',{Layout_Person,UNSIGNED8 RecPos{virtual(fileposition)}}, THOR);
        EXPORT FilePlus_FirstName_16 := DATASET(DATA_PATH+'FirstName_16',{Layout_Person,UNSIGNED8 RecPos{virtual(fileposition)}}, THOR);
        EXPORT FilePlus_FirstName_17 := DATASET(DATA_PATH+'FirstName_17',{Layout_Person,UNSIGNED8 RecPos{virtual(fileposition)}}, THOR);
        EXPORT FilePlus_FirstName_18 := DATASET(DATA_PATH+'FirstName_18',{Layout_Person,UNSIGNED8 RecPos{virtual(fileposition)}}, THOR);
        EXPORT FilePlus_FirstName_19 := DATASET(DATA_PATH+'FirstName_19',{Layout_Person,UNSIGNED8 RecPos{virtual(fileposition)}}, THOR);
        EXPORT FilePlus_FirstName_20 := DATASET(DATA_PATH+'FirstName_20',{Layout_Person,UNSIGNED8 RecPos{virtual(fileposition)}}, THOR);
        EXPORT FilePlus_FirstName_21 := DATASET(DATA_PATH+'FirstName_21',{Layout_Person,UNSIGNED8 RecPos{virtual(fileposition)}}, THOR);
        EXPORT FilePlus_FirstName_22 := DATASET(DATA_PATH+'FirstName_22',{Layout_Person,UNSIGNED8 RecPos{virtual(fileposition)}}, THOR);
        EXPORT FilePlus_FirstName_23 := DATASET(DATA_PATH+'FirstName_23',{Layout_Person,UNSIGNED8 RecPos{virtual(fileposition)}}, THOR);
        EXPORT FilePlus_FirstName_24 := DATASET(DATA_PATH+'FirstName_24',{Layout_Person,UNSIGNED8 RecPos{virtual(fileposition)}}, THOR);
        EXPORT FilePlus_FirstName_25 := DATASET(DATA_PATH+'FirstName_25',{Layout_Person,UNSIGNED8 RecPos{virtual(fileposition)}}, THOR);
        EXPORT FilePlus_FirstName_26 := DATASET(DATA_PATH+'FirstName_26',{Layout_Person,UNSIGNED8 RecPos{virtual(fileposition)}}, THOR);
        EXPORT FilePlus_FirstName_27 := DATASET(DATA_PATH+'FirstName_27',{Layout_Person,UNSIGNED8 RecPos{virtual(fileposition)}}, THOR);
        EXPORT FilePlus_FirstName_28 := DATASET(DATA_PATH+'FirstName_28',{Layout_Person,UNSIGNED8 RecPos{virtual(fileposition)}}, THOR);
        EXPORT FilePlus_FirstName_29 := DATASET(DATA_PATH+'FirstName_29',{Layout_Person,UNSIGNED8 RecPos{virtual(fileposition)}}, THOR);
        EXPORT FilePlus_FirstName_30 := DATASET(DATA_PATH+'FirstName_30',{Layout_Person,UNSIGNED8 RecPos{virtual(fileposition)}}, THOR);
        EXPORT FilePlus_FirstName_31 := DATASET(DATA_PATH+'FirstName_31',{Layout_Person,UNSIGNED8 RecPos{virtual(fileposition)}}, THOR);
        EXPORT FilePlus_FirstName_32 := DATASET(DATA_PATH+'FirstName_32',{Layout_Person,UNSIGNED8 RecPos{virtual(fileposition)}}, THOR);
        EXPORT FilePlus_FirstName_33 := DATASET(DATA_PATH+'FirstName_33',{Layout_Person,UNSIGNED8 RecPos{virtual(fileposition)}}, THOR);
        EXPORT FilePlus_FirstName_34 := DATASET(DATA_PATH+'FirstName_34',{Layout_Person,UNSIGNED8 RecPos{virtual(fileposition)}}, THOR);
        EXPORT FilePlus_FirstName_35 := DATASET(DATA_PATH+'FirstName_35',{Layout_Person,UNSIGNED8 RecPos{virtual(fileposition)}}, THOR);
        EXPORT FilePlus_FirstName_36 := DATASET(DATA_PATH+'FirstName_36',{Layout_Person,UNSIGNED8 RecPos{virtual(fileposition)}}, THOR);
        EXPORT FilePlus_FirstName_37 := DATASET(DATA_PATH+'FirstName_37',{Layout_Person,UNSIGNED8 RecPos{virtual(fileposition)}}, THOR);
        EXPORT FilePlus_FirstName_38 := DATASET(DATA_PATH+'FirstName_38',{Layout_Person,UNSIGNED8 RecPos{virtual(fileposition)}}, THOR);
        EXPORT FilePlus_FirstName_39 := DATASET(DATA_PATH+'FirstName_39',{Layout_Person,UNSIGNED8 RecPos{virtual(fileposition)}}, THOR);
        EXPORT FilePlus_FirstName_40 := DATASET(DATA_PATH+'FirstName_40',{Layout_Person,UNSIGNED8 RecPos{virtual(fileposition)}}, THOR);
        EXPORT FilePlus_FirstName_41 := DATASET(DATA_PATH+'FirstName_41',{Layout_Person,UNSIGNED8 RecPos{virtual(fileposition)}}, THOR);
        EXPORT FilePlus_FirstName_42 := DATASET(DATA_PATH+'FirstName_42',{Layout_Person,UNSIGNED8 RecPos{virtual(fileposition)}}, THOR);
        EXPORT FilePlus_FirstName_43 := DATASET(DATA_PATH+'FirstName_43',{Layout_Person,UNSIGNED8 RecPos{virtual(fileposition)}}, THOR);
        EXPORT FilePlus_FirstName_44 := DATASET(DATA_PATH+'FirstName_44',{Layout_Person,UNSIGNED8 RecPos{virtual(fileposition)}}, THOR);
        EXPORT FilePlus_FirstName_45 := DATASET(DATA_PATH+'FirstName_45',{Layout_Person,UNSIGNED8 RecPos{virtual(fileposition)}}, THOR);
        EXPORT FilePlus_FirstName_46 := DATASET(DATA_PATH+'FirstName_46',{Layout_Person,UNSIGNED8 RecPos{virtual(fileposition)}}, THOR);
        EXPORT FilePlus_FirstName_47 := DATASET(DATA_PATH+'FirstName_47',{Layout_Person,UNSIGNED8 RecPos{virtual(fileposition)}}, THOR);
        EXPORT FilePlus_FirstName_48 := DATASET(DATA_PATH+'FirstName_48',{Layout_Person,UNSIGNED8 RecPos{virtual(fileposition)}}, THOR);
        EXPORT FilePlus_FirstName_49 := DATASET(DATA_PATH+'FirstName_49',{Layout_Person,UNSIGNED8 RecPos{virtual(fileposition)}}, THOR);
        EXPORT FilePlus_FirstName_50 := DATASET(DATA_PATH+'FirstName_50',{Layout_Person,UNSIGNED8 RecPos{virtual(fileposition)}}, THOR);
        EXPORT FilePlus_FirstName_51 := DATASET(DATA_PATH+'FirstName_51',{Layout_Person,UNSIGNED8 RecPos{virtual(fileposition)}}, THOR);
        EXPORT FilePlus_FirstName_52 := DATASET(DATA_PATH+'FirstName_52',{Layout_Person,UNSIGNED8 RecPos{virtual(fileposition)}}, THOR);
        EXPORT FilePlus_FirstName_53 := DATASET(DATA_PATH+'FirstName_53',{Layout_Person,UNSIGNED8 RecPos{virtual(fileposition)}}, THOR);
        EXPORT FilePlus_FirstName_54 := DATASET(DATA_PATH+'FirstName_54',{Layout_Person,UNSIGNED8 RecPos{virtual(fileposition)}}, THOR);
        EXPORT FilePlus_FirstName_55 := DATASET(DATA_PATH+'FirstName_55',{Layout_Person,UNSIGNED8 RecPos{virtual(fileposition)}}, THOR);
        EXPORT FilePlus_FirstName_56 := DATASET(DATA_PATH+'FirstName_56',{Layout_Person,UNSIGNED8 RecPos{virtual(fileposition)}}, THOR);
        EXPORT FilePlus_FirstName_57 := DATASET(DATA_PATH+'FirstName_57',{Layout_Person,UNSIGNED8 RecPos{virtual(fileposition)}}, THOR);
        EXPORT FilePlus_FirstName_58 := DATASET(DATA_PATH+'FirstName_58',{Layout_Person,UNSIGNED8 RecPos{virtual(fileposition)}}, THOR);
        EXPORT FilePlus_FirstName_59 := DATASET(DATA_PATH+'FirstName_59',{Layout_Person,UNSIGNED8 RecPos{virtual(fileposition)}}, THOR);
        EXPORT FilePlus_FirstName_60 := DATASET(DATA_PATH+'FirstName_60',{Layout_Person,UNSIGNED8 RecPos{virtual(fileposition)}}, THOR);
        EXPORT FilePlus_FirstName_61 := DATASET(DATA_PATH+'FirstName_61',{Layout_Person,UNSIGNED8 RecPos{virtual(fileposition)}}, THOR);
        EXPORT FilePlus_FirstName_62 := DATASET(DATA_PATH+'FirstName_62',{Layout_Person,UNSIGNED8 RecPos{virtual(fileposition)}}, THOR);
        EXPORT FilePlus_FirstName_63 := DATASET(DATA_PATH+'FirstName_63',{Layout_Person,UNSIGNED8 RecPos{virtual(fileposition)}}, THOR);
        EXPORT FilePlus_FirstName_64 := DATASET(DATA_PATH+'FirstName_64',{Layout_Person,UNSIGNED8 RecPos{virtual(fileposition)}}, THOR);

        EXPORT IDX_FirstName_1 := INDEX(FilePlus_FirstName_1,{FirstName,RecPos},KEY_PATH+'FirstName_1');
        EXPORT IDX_FirstName_2 := INDEX(FilePlus_FirstName_2,{FirstName,RecPos},KEY_PATH+'FirstName_2');
        EXPORT IDX_FirstName_3 := INDEX(FilePlus_FirstName_3,{FirstName,RecPos},KEY_PATH+'FirstName_3');
        EXPORT IDX_FirstName_4 := INDEX(FilePlus_FirstName_4,{FirstName,RecPos},KEY_PATH+'FirstName_4');
        EXPORT IDX_FirstName_5 := INDEX(FilePlus_FirstName_5,{FirstName,RecPos},KEY_PATH+'FirstName_5');
        EXPORT IDX_FirstName_6 := INDEX(FilePlus_FirstName_6,{FirstName,RecPos},KEY_PATH+'FirstName_6');
        EXPORT IDX_FirstName_7 := INDEX(FilePlus_FirstName_7,{FirstName,RecPos},KEY_PATH+'FirstName_7');
        EXPORT IDX_FirstName_8 := INDEX(FilePlus_FirstName_8,{FirstName,RecPos},KEY_PATH+'FirstName_8');
        EXPORT IDX_FirstName_9 := INDEX(FilePlus_FirstName_9,{FirstName,RecPos},KEY_PATH+'FirstName_9');
        EXPORT IDX_FirstName_10 := INDEX(FilePlus_FirstName_10,{FirstName,RecPos},KEY_PATH+'FirstName_10');
        EXPORT IDX_FirstName_11 := INDEX(FilePlus_FirstName_11,{FirstName,RecPos},KEY_PATH+'FirstName_11');
        EXPORT IDX_FirstName_12 := INDEX(FilePlus_FirstName_12,{FirstName,RecPos},KEY_PATH+'FirstName_12');
        EXPORT IDX_FirstName_13 := INDEX(FilePlus_FirstName_13,{FirstName,RecPos},KEY_PATH+'FirstName_13');
        EXPORT IDX_FirstName_14 := INDEX(FilePlus_FirstName_14,{FirstName,RecPos},KEY_PATH+'FirstName_14');
        EXPORT IDX_FirstName_15 := INDEX(FilePlus_FirstName_15,{FirstName,RecPos},KEY_PATH+'FirstName_15');
        EXPORT IDX_FirstName_16 := INDEX(FilePlus_FirstName_16,{FirstName,RecPos},KEY_PATH+'FirstName_16');
        EXPORT IDX_FirstName_17 := INDEX(FilePlus_FirstName_17,{FirstName,RecPos},KEY_PATH+'FirstName_17');
        EXPORT IDX_FirstName_18 := INDEX(FilePlus_FirstName_18,{FirstName,RecPos},KEY_PATH+'FirstName_18');
        EXPORT IDX_FirstName_19 := INDEX(FilePlus_FirstName_19,{FirstName,RecPos},KEY_PATH+'FirstName_19');
        EXPORT IDX_FirstName_20 := INDEX(FilePlus_FirstName_20,{FirstName,RecPos},KEY_PATH+'FirstName_20');
        EXPORT IDX_FirstName_21 := INDEX(FilePlus_FirstName_21,{FirstName,RecPos},KEY_PATH+'FirstName_21');
        EXPORT IDX_FirstName_22 := INDEX(FilePlus_FirstName_22,{FirstName,RecPos},KEY_PATH+'FirstName_22');
        EXPORT IDX_FirstName_23 := INDEX(FilePlus_FirstName_23,{FirstName,RecPos},KEY_PATH+'FirstName_23');
        EXPORT IDX_FirstName_24 := INDEX(FilePlus_FirstName_24,{FirstName,RecPos},KEY_PATH+'FirstName_24');
        EXPORT IDX_FirstName_25 := INDEX(FilePlus_FirstName_25,{FirstName,RecPos},KEY_PATH+'FirstName_25');
        EXPORT IDX_FirstName_26 := INDEX(FilePlus_FirstName_26,{FirstName,RecPos},KEY_PATH+'FirstName_26');
        EXPORT IDX_FirstName_27 := INDEX(FilePlus_FirstName_27,{FirstName,RecPos},KEY_PATH+'FirstName_27');
        EXPORT IDX_FirstName_28 := INDEX(FilePlus_FirstName_28,{FirstName,RecPos},KEY_PATH+'FirstName_28');
        EXPORT IDX_FirstName_29 := INDEX(FilePlus_FirstName_29,{FirstName,RecPos},KEY_PATH+'FirstName_29');
        EXPORT IDX_FirstName_30 := INDEX(FilePlus_FirstName_30,{FirstName,RecPos},KEY_PATH+'FirstName_30');
        EXPORT IDX_FirstName_31 := INDEX(FilePlus_FirstName_31,{FirstName,RecPos},KEY_PATH+'FirstName_31');
        EXPORT IDX_FirstName_32 := INDEX(FilePlus_FirstName_32,{FirstName,RecPos},KEY_PATH+'FirstName_32');
        EXPORT IDX_FirstName_33 := INDEX(FilePlus_FirstName_33,{FirstName,RecPos},KEY_PATH+'FirstName_33');
        EXPORT IDX_FirstName_34 := INDEX(FilePlus_FirstName_34,{FirstName,RecPos},KEY_PATH+'FirstName_34');
        EXPORT IDX_FirstName_35 := INDEX(FilePlus_FirstName_35,{FirstName,RecPos},KEY_PATH+'FirstName_35');
        EXPORT IDX_FirstName_36 := INDEX(FilePlus_FirstName_36,{FirstName,RecPos},KEY_PATH+'FirstName_36');
        EXPORT IDX_FirstName_37 := INDEX(FilePlus_FirstName_37,{FirstName,RecPos},KEY_PATH+'FirstName_37');
        EXPORT IDX_FirstName_38 := INDEX(FilePlus_FirstName_38,{FirstName,RecPos},KEY_PATH+'FirstName_38');
        EXPORT IDX_FirstName_39 := INDEX(FilePlus_FirstName_39,{FirstName,RecPos},KEY_PATH+'FirstName_39');
        EXPORT IDX_FirstName_40 := INDEX(FilePlus_FirstName_40,{FirstName,RecPos},KEY_PATH+'FirstName_40');
        EXPORT IDX_FirstName_41 := INDEX(FilePlus_FirstName_41,{FirstName,RecPos},KEY_PATH+'FirstName_41');
        EXPORT IDX_FirstName_42 := INDEX(FilePlus_FirstName_42,{FirstName,RecPos},KEY_PATH+'FirstName_42');
        EXPORT IDX_FirstName_43 := INDEX(FilePlus_FirstName_43,{FirstName,RecPos},KEY_PATH+'FirstName_43');
        EXPORT IDX_FirstName_44 := INDEX(FilePlus_FirstName_44,{FirstName,RecPos},KEY_PATH+'FirstName_44');
        EXPORT IDX_FirstName_45 := INDEX(FilePlus_FirstName_45,{FirstName,RecPos},KEY_PATH+'FirstName_45');
        EXPORT IDX_FirstName_46 := INDEX(FilePlus_FirstName_46,{FirstName,RecPos},KEY_PATH+'FirstName_46');
        EXPORT IDX_FirstName_47 := INDEX(FilePlus_FirstName_47,{FirstName,RecPos},KEY_PATH+'FirstName_47');
        EXPORT IDX_FirstName_48 := INDEX(FilePlus_FirstName_48,{FirstName,RecPos},KEY_PATH+'FirstName_48');
        EXPORT IDX_FirstName_49 := INDEX(FilePlus_FirstName_49,{FirstName,RecPos},KEY_PATH+'FirstName_49');
        EXPORT IDX_FirstName_50 := INDEX(FilePlus_FirstName_50,{FirstName,RecPos},KEY_PATH+'FirstName_50');
        EXPORT IDX_FirstName_51 := INDEX(FilePlus_FirstName_51,{FirstName,RecPos},KEY_PATH+'FirstName_51');
        EXPORT IDX_FirstName_52 := INDEX(FilePlus_FirstName_52,{FirstName,RecPos},KEY_PATH+'FirstName_52');
        EXPORT IDX_FirstName_53 := INDEX(FilePlus_FirstName_53,{FirstName,RecPos},KEY_PATH+'FirstName_53');
        EXPORT IDX_FirstName_54 := INDEX(FilePlus_FirstName_54,{FirstName,RecPos},KEY_PATH+'FirstName_54');
        EXPORT IDX_FirstName_55 := INDEX(FilePlus_FirstName_55,{FirstName,RecPos},KEY_PATH+'FirstName_55');
        EXPORT IDX_FirstName_56 := INDEX(FilePlus_FirstName_56,{FirstName,RecPos},KEY_PATH+'FirstName_56');
        EXPORT IDX_FirstName_57 := INDEX(FilePlus_FirstName_57,{FirstName,RecPos},KEY_PATH+'FirstName_57');
        EXPORT IDX_FirstName_58 := INDEX(FilePlus_FirstName_58,{FirstName,RecPos},KEY_PATH+'FirstName_58');
        EXPORT IDX_FirstName_59 := INDEX(FilePlus_FirstName_59,{FirstName,RecPos},KEY_PATH+'FirstName_59');
        EXPORT IDX_FirstName_60 := INDEX(FilePlus_FirstName_60,{FirstName,RecPos},KEY_PATH+'FirstName_60');
        EXPORT IDX_FirstName_61 := INDEX(FilePlus_FirstName_61,{FirstName,RecPos},KEY_PATH+'FirstName_61');
        EXPORT IDX_FirstName_62 := INDEX(FilePlus_FirstName_62,{FirstName,RecPos},KEY_PATH+'FirstName_62');
        EXPORT IDX_FirstName_63 := INDEX(FilePlus_FirstName_63,{FirstName,RecPos},KEY_PATH+'FirstName_63');
        EXPORT IDX_FirstName_64 := INDEX(FilePlus_FirstName_64,{FirstName,RecPos},KEY_PATH+'FirstName_64');
        
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
