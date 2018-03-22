##These are the libraries necessary to do the claims mapping. 
Connect_Packages <- function(){
        ipak <- function(pkg){
          new.pkg <- pkg[!(pkg %in% installed.packages()[, "Package"])]
          if (length(new.pkg)) 
            install.packages(new.pkg, dependencies = TRUE)
          sapply(pkg, require, character.only = TRUE)
        }
Packages <- c("RODBC", "dplyr", "tidyr", "sqldf", "stringr")
ipak(Packages)
EDW=odbcConnect("EDW_PROD")
EDW = return(EDW)
}

##Data pull for just Claims
CLAIMS_PULL <- function(){ 
            ##Pulls memebers who have qualified claims.  
            All_Members <- sqlQuery(EDW, 
                                    "select*
                                     from rro_prod.members_allplans_2016")
            
            All_Members$metal <- trimws(as.character(All_Members$metal), "both")
            
            ##pulls in only their dw_indivl_key, metal, age_first, gender, variant_cd
            All_Members_ID <- All_Members %>% select(dw_indivl_key, metal, age_first, gender, variant_cd) %>% unique()
            
            All_Members_ID$metal <- trimws(as.character(All_Members_ID$metal), "both") 
            
            ##Pull all members who have had qualified claims 
            Diags_Claims  <- sqlQuery(EDW, 
                                      "select*
                                       from rro_prod.diags_accepted_filter_2016")
            Diags_Claims <- na.omit(Diags_Claims)
            Diags_Split<- as.character(Diags_Claims$DIAG_CD)
            Diags_Split = unlist(strsplit(Diags_Split, " "))
            Diags_Split = Diags_Split[Diags_Split != ""]
            Diags_Claims <- cbind(Diags_Claims, Diags_Split)
            
            Diags_Claims$metal <- trimws(as.character(Diags_Claims$metal), "both")
            
            ##makes a copy dataframe for claims
            Claims <- Diags_Claims
            
            ##ALL MEMBERS & CLAIMS
            Diags_Claims <-  merge(All_Members_ID, Diags_Claims, by = c("dw_indivl_key", "metal", "gender", "age_first"), all.x =  TRUE)
            
            ##DIAGS_ADDED
            Adds <- sqlQuery(EDW,
                              "
                              select* from  rro_prod.adddelete_dwclaim
                              "
                              )
            
            Data_Claims <- return(list(All_Members, All_Members_ID, Claims, Diags_Claims, Adds))
}

##Pulls in CC information which provides information on to to link to claims.  
Diag_HCC_Clean <- function(){
  
          Diag_HCC <- sqlQuery(EDW,
                               "SEL * FROM rro_prod.update_hhs_table3") 
          ##Teredata automatically coerces DIAG codes into 7 characters even if there are spaces. (Originally it was a vector)
          
          Diag_Split<- as.character(Diag_HCC$diag_cd)
          Diag_Split = unlist(strsplit(Diag_Split, " "))
          Diag_Split = Diag_Split[Diag_Split != ""]
          
          Diag_HCC <- cbind(Diag_HCC, Diag_Split)
          Diag_HCC <- Diag_HCC %>% 
            select(diag_cd, Diag_Split, diag_label, CC, Additional_CC, fy_start, fy_end, Age_L, Age_U, Sex_Cond)
          
          ##Change start and end date to date format. 
          Diag_HCC$fy_start <- as.Date(Diag_HCC$fy_start)
          Diag_HCC$fy_end <- as.Date(Diag_HCC$fy_end)
          
          ##recoding the Sex condition variable to M, F, NSA(Not Sex Associated) -->indicator variable if Diagnosis is related to Sex
          Sex_Cod <- ifelse(is.na(Diag_HCC$Sex_Cond) == TRUE, 'NSA', 
                            ifelse(Diag_HCC$Sex_Cond == 'f', 'F', 'M')) 
          Diag_HCC <- cbind(Diag_HCC, Sex_Cod)
          Diag_HCC$Sex_Cond <- NULL
          Diag_HCC <- return(Diag_HCC)
  
}

##Links claims through CC 
CLAIM_CC_LINK<- function(Diags_Claims, Diag_HCC){

            Diags_Claims <- Diags_Claims
            Diag_HCC <- Diag_HCC
            
            CC_Claims  <- sqldf("
                            SELECT
                                d1.dw_indivl_key,
                                d1.dw_mbr_key,
                                d1.gender AS gndr_cd,
                                d1.variant_cd,
                                d1.metal,
                                d1.clm_dt,
                                d1.age_first,
                                d1.dw_clm_key,
                                d1.Diag_CD,
                                d2.Diag_Split As DIAG_CD,
                                d2.CC,
                                d2.Additional_CC,
                                d2.Sex_Cod
                                
                                FROM Diags_Claims AS d1 
                                
                                LEFT JOIN Diag_HCC d2
                                ON d1.Diags_Split = d2.Diag_Split
                                AND d1.age_first <= d2.Age_U
                                AND d1.age_first >= d2.Age_L
                                AND d1.clm_dt BETWEEN d2.fy_start AND d2.fy_end
                                
                                WHERE d1.age_first >= 2
                                
                                ")
            
            
            CC_Claims$DIAG_CD <- NULL
            
            ##Pulling in additional CC
            
            Additional_CC <- sqldf(
              "
              SELECT*
              FROM CC_Claims
              WHERE Additional_CC IS NOT NULL
              "
            )
            
            Additional_CC$CC <- NULL
            names(Additional_CC)[names(Additional_CC) == 'Additional_CC'] <- 'CC'
            CC_Claims$Additional_CC <-NULL
            CC_Claims <- rbind(CC_Claims, Additional_CC)
            
            ##If there is a mismatch in sex condition then it will remove the clm and keep the demographic information
            MisMatch <- sqldf("
                              SELECT*
                              FROM CC_Claims
                              WHERE (CC_Claims.Sex_Cod = 'M' and CC_Claims.gndr_cd = 'F')
                              OR (CC_Claims.Sex_Cod = 'F' and CC_Claims.gndr_cd = 'M')
                              ")
            
            MisMatch$clm_dt <- NA
            MisMatch$dw_clm_key <- NA
            MisMatch$DIAG_CD <- NA
            MisMatch$CC <- NA
            MisMatch$Sex_Cod <- NA
            
            
            CC_Claims<- sqldf(" 
                                    SELECT*
                                    FROM CC_Claims 
                                    WHERE CC_Claims.Sex_cod is NULL
            
                                    UNION 
            
                                    SELECT*
                                    FROM CC_Claims
                                    WHERE (CC_Claims.Sex_Cod = 'F' and CC_Claims.gndr_cd = 'F')
                                      OR (CC_Claims.Sex_Cod = 'M' and CC_Claims.gndr_cd = 'M')
                                      OR (CC_Claims.Sex_Cod = 'NSA' and CC_Claims.gndr_cd = 'F')
                                      OR (CC_Claims.Sex_Cod = 'NSA' and CC_Claims.gndr_cd = 'M')
                           ")
            
            CC_Claims <- rbind(CC_Claims, MisMatch)
            
            ##we want the unique CC's per person
            
            CC_Claims$age_first<- as.numeric(CC_Claims$age_first)
            
            CC_Claims_Unique<- CC_Claims %>% 
              select(dw_indivl_key, dw_mbr_key, gndr_cd, metal, age_first, DIAG_CD, CC, variant_cd) %>% 
              group_by(dw_indivl_key, gndr_cd, metal) %>%
              mutate(age = min(age_first, na.rm = TRUE)) %>%
              unique()
            
            CC_Claims_Unique$dw_mbr_key <- NULL;
            CC_Claims_Unique$dw_mbr_key <- NULL;
            CC_Claims_Unique$h_mpi <- NULL;
            CC_Claims_Unique$age_first <- NULL;
            
            CC_Claims_Unique <- return(CC_Claims_Unique)

}

##Applies heiarchy methodlogy. 
Heiarchy_Methodology <- function(CC_Claims_Unique){

        CC_Claims_Unique <- CC_Claims_Unique
          
        HCC_H <- sqlQuery(EDW, 
                          "
                          SEL * 
                          FROM rro_prod.update_hhs_table4
                          ")
        ##This is used to filter hierarchy for the year we are analazying--> needs to be changed if year changes
        Year <- '2016'
        HCC_H_Y <- HCC_H %>% filter(yr == Year) %>%  arrange(hcc)
        
        CC_DROP <- sqldf(
          "SELECT a.dw_indivl_key, b.d_hcc1, b.d_hcc2, b.d_hcc3, b.d_hcc4, b.d_hcc5, b.d_hcc6, b.d_hcc7
          FROM CC_Claims_Unique a LEFT JOIN HCC_H_Y b ON a.CC = b.hcc")
        
        ##Reformat data from wide to tall
        CC_DROP <- gather(CC_DROP, d_hcc, CC, -dw_indivl_key) %>% distinct() %>% filter(!is.na(CC))
        
        ##Remove unnecessary CCs
        CLAIMS_HEIRARCHY_MAP <- anti_join( CC_Claims_Unique, CC_DROP, by = c('dw_indivl_key', 'CC') )
        CLAIMS_HEIRARCHY_MAP <- return(CLAIMS_HEIRARCHY_MAP)

}

##Age Clustering 
CLAIMS_AGE <- function(CLAIMS_HEIRARCHY_MAP){
  
          CLAIMS_HEIARCHY_AGE <- CLAIMS_HEIRARCHY_MAP %>% 
            mutate(Child_Adult_IND = ifelse(age <= 20, 1,0)) %>% 
            mutate(Child_Cluster = ifelse(between(age, 2,4),'AGE_LAST_2_4',
                                          ifelse(between(age,5,9),'AGE_LAST_5_9', 
                                                 ifelse(between(age, 10, 14),'AGE_LAST_10_14',
                                                        ifelse(between(age, 15, 20), 'AGE_LAST_15_20', 'ADULT'))))) %>%
            mutate(Adult_Cluster = ifelse(between(age, 21, 24), 'AGE_LAST_21_24',
                                          ifelse(between(age, 25, 29), 'AGE_LAST_25_29',
                                                 ifelse(between(age, 30, 34), 'AGE_LAST_30_34',
                                                        ifelse(between(age, 35, 39), 'AGE_LAST_35_39',
                                                               ifelse(between(age, 40, 44), 'AGE_LAST_40_44',
                                                                      ifelse(between(age, 45, 49), 'AGE_LAST_45_49',
                                                                             ifelse(between(age, 50, 54), 'AGE_LAST_50_54',
                                                                                    ifelse(between(age, 55, 59), 'AGE_LAST_55_59', 
                                                                                           ifelse(age >= 60, 'AGE_LAST_60_GT', 'CHILD'))))))))))
          
  
  
  
}

##Adding in the Severe Diags
HCC_Severe <- function(CLAIMS_HEIARCHY_AGE){
    
            ##Adding in the Severe Variable --> ADULTS 
            ##Corresponds to HCC 02, 42, 120, 122, 125, 126, 127, 156
          
            SEVERE_HCC <- c(02, 42, 120, 122, 125, 126, 127, 156)
            CLAIMS_ADULT <-  CLAIMS_HEIARCHY_AGE %>% 
            filter(Child_Adult_IND == 0) %>% 
            mutate(SEVERE_IND = ifelse(CC %in% SEVERE_HCC , 1, 0))
          
            ##Maps at a member level by catagorizing which member is "SEVERE", they are Severe if they have a condition which is SEVERE. 
          
            MEM_SEVERE <- sqldf("
                                              SELECT
                                              Distinct dw_indivl_key
                                              FROM CLAIMS_ADULT
                                              WHERE SEVERE_IND = '1' 
                                                                      ")   
            MEM_SEVERE <- as.vector(t(MEM_SEVERE))
          
            CLAIMS_ADULT <- CLAIMS_ADULT %>% 
            mutate(SEVERE_MEM = ifelse(dw_indivl_key %in% MEM_SEVERE,1, 0))
          
            CLAIMS_ADULT <- return(CLAIMS_ADULT)
}

##Recoding
G_HCC_RECODE <- function(CLAIMS_ADULTS){
  
      G01 <- c(19,20,21)
      G02A <- c(26,27,29,30)
      G03 <- c(54, 55)
      G04 <- c(61, 62)
      G06 <- c(67, 68)
      G07 <- c(69, 70, 71)
      G08 <- c(73, 74)
      G09 <- c(81, 82)
      G10 <- c(106, 107)
      G11 <- c(108, 109)
      G12 <- c(117, 119)
      G14 <- c(128, 129)
      G15 <- c(160, 161)
      G16 <- c(187, 188)
      G17 <- c(203, 204, 205)
      G18 <- c(207, 208, 209)
      
      
      CLAIMS_ADULTS$CC <- as.character(CLAIMS_ADULTS$CC)
      
      CLAIMS_ADULTS <- CLAIMS_ADULTS %>%
        mutate(CC_G = ifelse(CC %in% G01, 'G01', 
                             ifelse(CC %in% G02A, 'G02A',
                                    ifelse(CC %in% G03, 'G03',
                                           ifelse(CC %in% G04, 'G04', 
                                                  ifelse(CC %in% G06, 'G06',
                                                         ifelse(CC %in% G07, 'G07', 
                                                                ifelse(CC %in% G08, 'G08',
                                                                       ifelse(CC %in% G09, 'G09',
                                                                              ifelse(CC %in% G10, 'G10',
                                                                                     ifelse(CC %in% G11, 'G11',
                                                                                            ifelse(CC %in% G12, 'G12',
                                                                                                   ifelse(CC %in% G14, 'G14',
                                                                                                          ifelse(CC %in% G15, 'G15',
                                                                                                                 ifelse(CC %in% G16, 'G16',
                                                                                                                        ifelse(CC %in% G17, 'G17',
                                                                                                                               ifelse(CC %in% G18, 'G18', CC)))))))))))))))))
      
      
      
      ##Updating CC where it captures Severe + HCC = SEVERE_V3_HHS_HCC006
      
      CC_INT <- c(6, 8, 9, 10, 115, 135, 145, 35, 38, 153, 154, 163, 253)
      G_INT <- c('G06', 'G08', 'G03')
      
      CLAIMS_ADULTS <- CLAIMS_ADULTS %>%
        mutate(CC_SEVERE = ifelse(SEVERE_MEM == 1 & CC_G %in% CC_INT, paste("SEVERE_V3_HHS_HCC", CC_G, sep = "_"), 
                                  ifelse(SEVERE_MEM == 1 & CC_G %in% G_INT, paste("SEVERE_V3_X", CC_G, sep = "_"), CC_G)))
}

##INT H
INT_H <- function(CLAIMS_ADULTS){
  
  INT_GROUP_H <- c("SEVERE_V3_HHS_HCC_6",
                   "SEVERE_V3_HHS_HCC_8",
                   "SEVERE_V3_HHS_HCC_9",
                   "SEVERE_V3_HHS_HCC_10",
                   "SEVERE_V3_HHS_HCC_115",
                   "SEVERE_V3_HHS_HCC_135",
                   "SEVERE_V3_HHS_HCC_145",
                   "SEVERE_V3_X_G06",
                   "SEVERE_V3_X_G08")
  
  ##Catagorizes the Diags which are in INT group H
  CLAIMS_ADULTS <- CLAIMS_ADULTS %>%
    mutate(INT_GROUP_H_IND = ifelse(CC_SEVERE %in% INT_GROUP_H, 1,0))
  
  
   MEM_INT_GROUP_H <- sqldf("
                                                SELECT
                                                Distinct dw_indivl_key
                                                FROM CLAIMS_ADULTS
                                                WHERE INT_GROUP_H_IND = '1' 
                                                ")   
  
  MEM_INT_GROUP_H<- as.vector(t(MEM_INT_GROUP_H))
  
  CLAIMS_ADULTS <- CLAIMS_ADULTS %>% 
    mutate(MEM_INT_H = ifelse(dw_indivl_key %in% MEM_INT_GROUP_H, 1, 0)) 
  
}

##INT M
INT_M <- function(CLAIMS_ADULTS){
  
  INT_GROUP_M <- c("SEVERE_V3_HHS_HCC_35",
                   "SEVERE_V3_HHS_HCC_38",
                   "SEVERE_V3_HHS_HCC_153",
                   "SEVERE_V3_HHS_HCC_154",
                   "SEVERE_V3_HHS_HCC_163",
                   "SEVERE_V3_HHS_HCC_253",
                   "SEVERE_V3_X_G03")
  
  ##Maps the diagnosis which catagorize for INT GROUP M
  CLAIMS_ADULTS  <- CLAIMS_ADULTS %>%
    mutate(INT_GROUP_M_IND = ifelse(INT_GROUP_H_IND == 0 & CC_SEVERE %in% INT_GROUP_M, 1, 0))
  
  ##Which Memebers are catagorize for INT GROUP M
  MEM_INT_GROUP_M <- sqldf("
                                                 SELECT
                                                 Distinct dw_indivl_key
                                                 FROM CLAIMS_ADULTS
                                                 WHERE INT_GROUP_M_IND = '1' 
                                                 ")   
  
  MEM_INT_GROUP_M<- as.vector(t(MEM_INT_GROUP_M))
  
  CLAIMS_ADULTS <- CLAIMS_ADULTS %>% 
    mutate(MEM_INT_M = ifelse(dw_indivl_key %in% MEM_INT_GROUP_M, 1, 0))
  
}

##CLAIMS children recode. 
CLAIMS_Children_Recode <- function(CLAIMS_HEIARCHY_AGE){
  
  G01 <- c(19,20,21)
  G02 <- c(26,27,28,29,30)
  G03 <- c(54, 55)
  G04 <- c(61, 62)
  G06 <- c(67, 68)
  G07 <- c(69, 70, 71)
  G08 <- c(73, 74)
  G09 <- c(81,82)
  G10 <- c(106, 107)
  G11 <- c(108, 109)
  G12 <- c(117, 119)
  G13 <- c(126, 127)
  G14 <- c(128, 129)
  G15 <- c(160, 161)
  G16 <- c(187, 188)
  G17 <- c(203, 204, 205)
  G18 <- c(207, 208, 209)
  
  CLAIMS_Children <-  CLAIMS_HEIARCHY_AGE %>% 
    filter(Child_Adult_IND == 1) 
  
  CLAIMS_Children$CC <- as.character(CLAIMS_Children$CC)  
  
  CLAIMS_Children <- CLAIMS_Children %>%
    mutate(CC_G = ifelse(CC %in% G01, 'G01', 
                         ifelse(CC %in% G02, 'G02',
                                ifelse(CC %in% G03, 'G03', 
                                       ifelse(CC %in% G04, 'G04',
                                              ifelse(CC %in% G06, 'G06',
                                                     ifelse(CC %in% G07, 'G07',
                                                            ifelse(CC %in% G08, 'G08',
                                                                   ifelse(CC %in% G09, 'G09',
                                                                          ifelse(CC %in% G10, 'G10',
                                                                                 ifelse(CC %in% G11, 'G11',
                                                                                        ifelse(CC %in% G12, 'G12',
                                                                                               ifelse(CC %in% G13, 'G13',
                                                                                                      ifelse(CC %in% G14, 'G14',
                                                                                                             ifelse(CC %in% G15, 'G15',
                                                                                                                    ifelse(CC %in% G16, 'G16', 
                                                                                                                           ifelse(CC %in% G17, 'G17',
                                                                                                                                  ifelse(CC %in% G18, 'G18', CC))))))))))))))))))
}

##Age_Sex Risk Score
Age_Sex <- function(){
  
  Risk_Score <- read.csv("Risk Score .csv")

  Age_Risk_M <- filter(Risk_Score, grepl("MAGE", Variable, fixed = TRUE))
  Sex <- substring(Age_Risk_M$Variable, 1, 1)
  Age_Risk_M$Variable <- substring(Age_Risk_M$Variable,2)
  Age_Risk_M <- cbind(Age_Risk_M, Sex)
  
  Age_Risk_F <- filter(Risk_Score, grepl("FAGE", Variable, fixed = TRUE))
  Sex <- substring(Age_Risk_F$Variable, 1, 1)
  Age_Risk_F$Variable <- substring(Age_Risk_F$Variable, 2)
  Age_Risk_F <- cbind(Age_Risk_F, Sex)
  
  Age_Sex_Risk <- rbind(Age_Risk_M, Age_Risk_F)
  Age_Sex_Risk <- return(Age_Sex_Risk)
  
}

##Getting Risk Score of Recoded CC
CC_G_RiskScore <- function(){

CC_G <- function(){
  
  Risk_Score <- read.csv("Risk Score .csv")
  
  CC_Risk <- filter(Risk_Score, grepl("HHS", Variable, fixed = TRUE))
  CC <-do.call(rbind, str_split(CC_Risk$Variable, 'HHS_HCC'))[,2]
  CC_Risk$Variable <- CC
  
  G_Risk <- filter(Risk_Score, grepl("^G", Variable))
  CC_G_Risk <-rbind(CC_Risk, G_Risk) 
  CC_G_Risk <- return(CC_G_Risk)
  
}

CC_G_Risk <- CC_G()

CC_G_Risk <- CC_G_Risk %>% 
  mutate(Child_Adult_IND = ifelse(Model == 'Adult', 0, 1))

##Truncate the zeros off of the "Variable"  HCC
CC <- CC_G_Risk$Variable
No_Trunct <- CC[grep("G", CC)]
CC_G_Risk$Variable <- ifelse(CC %in% No_Trunct, CC, 
                             gsub("(?<![0-9])0+", "", CC, perl = TRUE))


CC_G_Risk <- return(CC_G_Risk)

}

##Getting interaction Scores  (INT_H_Risk & INT_M_Risk)
INT_RISK <- function(){
Risk_Score <- read.csv("Risk Score .csv")
INT_H_RISK <- filter(Risk_Score, Variable ==  'INT_GROUP_H' )
INT_M_RISK <- filter(Risk_Score, Variable ==  'INT_GROUP_M' )

Interaction_Risk <- return(list(INT_H_RISK, INT_M_RISK))

}

##Getting Claims risk for their CC. If the member does n ot have any risk since he or she has no claims
##then it will convert those risk scores to zero. 

Claims_CC_Risk <- function(CLAIMS_ADULTS, CC_G_Risk){

##Joins to CC_G_Risk to get risk for each claim for their CC
CLAIMS_Adult_Score <- sqldf(
  "SELECT* 
  FROM CLAIMS_ADULTS A
  LEFT JOIN CC_G_Risk B 
  ON A.CC_G = B.Variable
  AND A.Child_Adult_IND = B.Child_Adult_IND
  "
)

CLAIMS_Adult_Score$Child_Adult_IND <- NULL;
CLAIMS_Adult_Score$metal <- trimws(as.character(CLAIMS_Adult_Score$metal), "both")

##if they are NA they replace with 0 
CLAIMS_Adult_Score$Platinum.Level[is.na(CLAIMS_Adult_Score$Platinum.Level)] <- 0
CLAIMS_Adult_Score$Gold.Level[is.na(CLAIMS_Adult_Score$Gold.Level)] <- 0
CLAIMS_Adult_Score$Silver.Level[is.na(CLAIMS_Adult_Score$Silver.Level)] <- 0
CLAIMS_Adult_Score$Bronze.Level[is.na(CLAIMS_Adult_Score$Bronze.Level)] <- 0
CLAIMS_Adult_Score$Catastrophic.Level[is.na(CLAIMS_Adult_Score$Catastrophic.Level)] <- 

CLAIMS_Adult_Score <- return(CLAIMS_Adult_Score)

}

CLAIMS_Adults_Score_Aggregate <- function(CLAIMS_Adults_Score){
  
  CLAIMS_Adults_Score <- CLAIMS_Adults_Score
  
  Age_Sex_Risk <- Age_Sex_Risk
  
  CLAIMS_Adults_Score_CC   <- CLAIMS_Adults_Score %>% 
    mutate(Risk_Score = ifelse(metal == 'Silver', Silver.Level,
                               ifelse(metal == 'Gold', Gold.Level,
                                      ifelse(metal == 'Platinum', Platinum.Level,
                                             ifelse(metal == 'Bronze', Bronze.Level, Catastrophic.Level)))))
  
  ##Aggregates the risk for the CC's for each member, by using the summarize dplyr function.                                         
  CLAIMS_Adults_Score_Agg<- CLAIMS_Adults_Score_CC %>% 
    group_by(dw_indivl_key, Adult_Cluster, gndr_cd, metal, variant_cd, MEM_INT_H, MEM_INT_M) %>% 
    summarize(Aggregate_HCC  = sum(Risk_Score));
  
  CLAIMS_Adult_Score_Agg <- return(CLAIMS_Adults_Score_Agg)
  
  
}

CLAIMS_Adults_Score_AggC <- function(CLAIMS_Adults_Score_Agg){
  
  CLAIMS_Adults_Score_Agg <-  CLAIMS_Adults_Score_Agg %>% 
    mutate(AGEGEN_Risk  =  ifelse(metal == 'Bronze', Bronze.Level,
                                  ifelse(metal == 'Silver', Silver.Level,
                                         ifelse(metal == 'Gold', Gold.Level,
                                                ifelse(metal == 'Platinum', Platinum.Level, Catastrophic.Level))))) %>%
    mutate(INT_H_Risk = ifelse(MEM_INT_H == 1 & metal == 'Bronze', INT_H_RISK$Bronze.Level, 
                               ifelse(MEM_INT_H == 1 & metal == 'Silver', INT_H_RISK$Silver.Level,
                                      ifelse(MEM_INT_H == 1 & metal == 'Gold', INT_H_RISK$Gold.Level,
                                             ifelse(MEM_INT_H == 1 & metal == 'Platinum', INT_H_RISK$Platinum.Level,
                                                    ifelse(MEM_INT_H == 1 & metal == 'Catastroph', INT_H_RISK$Catastrophic.Level, 0)))))) %>%
    mutate(INT_M_Risk = ifelse(MEM_INT_M == 1 & metal == 'Bronze', INT_M_RISK$Bronze.Level, 
                               ifelse(MEM_INT_M == 1 & metal == 'Silver', INT_M_RISK$Silver.Level,
                                      ifelse(MEM_INT_M == 1 & metal == 'Gold', INT_M_RISK$Gold.Level,
                                             ifelse(MEM_INT_M == 1 & metal == 'Platinum', INT_M_RISK$Platinum.Level,
                                                    ifelse(MEM_INT_M == 1 & metal == 'Catastroph', INT_M_RISK$Catastrophic.Level, 0)))))) %>%
    mutate(Total_Risk = Aggregate_HCC + AGEGEN_Risk + INT_H_Risk + INT_M_Risk) %>%
    mutate(Total_Risk_VC = ifelse(variant_cd == 6 & metal == 'Silver', Total_Risk*1.12, 
                                  ifelse(variant_cd == 5 & metal == 'Silver', Total_Risk*1.12,
                                         ifelse(variant_cd == 2 & metal == 'Gold', Total_Risk*1.07,
                                                ifelse(variant_cd == 2 & metal == 'Silver', Total_Risk*1.12,
                                                       ifelse(variant_cd == 2 & metal == 'Bronze', Total_Risk*1.15,
                                                              ifelse(variant_cd == 3 & metal == 'Gold', Total_Risk*1.07,
                                                                     ifelse(variant_cd == 3 & metal == 'Silver', Total_Risk*1.12,
                                                                            ifelse(variant_cd == 3 & metal == 'Bronze', Total_Risk*1.15, Total_Risk)))))))))
  
  
  CLAIMS_Adults_Score_Agg <- return(CLAIMS_Adults_Score_Agg)
  
}

###Children
Claims_CH_Risk <- function(CLAIMS_Children, CC_G_RISK){
  
  ##Joins to CC_G_Risk to get risk for each claim for their CC

  CLAIMS_Children_Score <- sqldf(
    "SELECT* 
                          FROM CLAIMS_Children A
                          LEFT JOIN CC_G_Risk B 
                          ON A.CC_G = B.Variable
                          AND A.Child_Adult_IND = B.Child_Adult_IND"
  )
  
  CLAIMS_Children_Score$Child_Adult_IND <- NULL;
  CLAIMS_Children_Score$metal <- trimws(as.character(CLAIMS_Children_Score$metal), "both")
  
  ##if they are NA they replace with 0 
  CLAIMS_Children_Score$Platinum.Level[is.na(CLAIMS_Children_Score$Platinum.Level)] <- 0
  CLAIMS_Children_Score$Gold.Level[is.na(CLAIMS_Children_Score$Gold.Level)] <- 0
  CLAIMS_Children_Score$Silver.Level[is.na(CLAIMS_Children_Score$Silver.Level)] <- 0
  CLAIMS_Children_Score$Bronze.Level[is.na(CLAIMS_Children_Score$Bronze.Level)] <- 0
  CLAIMS_Children_Score$Catastrophic.Level[is.na(CLAIMS_Children_Score$Catastrophic.Level)] <- 0
    
  CLAIMS_Children_Score <- return(CLAIMS_Children_Score)

  
}

CLAIMS_Children_Score_Aggregate <- function(CLAIMS_Children_Score){
  
  CLAIMS_Children_Score_CC   <- CLAIMS_Children_Score %>% 
    mutate(Risk_Score = ifelse(metal == 'Bronze', Bronze.Level, 
                               ifelse(metal == 'Gold', Gold.Level,
                                      ifelse(metal == 'Platinum', Platinum.Level,
                                             ifelse(metal == 'Silver', Silver.Level, Catastrophic.Level)))))
  
  ##Aggregates the risk for the CC's for each member, by using the summarize dplyr function. 
  CLAIMS_Children_Score_Agg<- CLAIMS_Children_Score_CC %>% 
    group_by(dw_indivl_key, Child_Cluster, gndr_cd, metal, variant_cd) %>% 
    summarize(Aggregate_HCC  = sum(Risk_Score))
  
  CLAIMS_Children_Score_Agg <- return(CLAIMS_Children_Score_Agg)
  
}

CLAIMS_Children_Score_AggC <- function(CLAIMS_Children_Score_Agg){
  
  CLAIMS_Children_Score_Agg <-  CLAIMS_Children_Score_Agg %>% 
    mutate(AGEGEN_Risk  =  ifelse(metal == 'Bronze', Bronze.Level,
                                  ifelse(metal == 'Silver', Silver.Level,
                                         ifelse(metal == 'Gold', Gold.Level,
                                                ifelse(metal == 'Platinum', Platinum.Level, Catastrophic.Level)))))%>%
    mutate(Total_Risk = Aggregate_HCC + AGEGEN_Risk) %>%
    mutate(Total_Risk_VC = ifelse(variant_cd == 6 & metal == 'Silver', Total_Risk*1.12, 
                                  ifelse(variant_cd == 5 & metal == 'Silver', Total_Risk*1.12,
                                         ifelse(variant_cd == 2 & metal == 'Gold', Total_Risk*1.07,
                                                ifelse(variant_cd == 2 & metal == 'Silver', Total_Risk*1.12,
                                                       ifelse(variant_cd == 2 & metal == 'Bronze', Total_Risk*1.15,
                                                              ifelse(variant_cd == 3 & metal == 'Gold', Total_Risk*1.07,
                                                                     ifelse(variant_cd == 3 & metal == 'Silver', Total_Risk*1.12,
                                                                            ifelse(variant_cd == 3 & metal == 'Bronze', Total_Risk*1.15, Total_Risk)))))))))
  
  
  
  
  
}








