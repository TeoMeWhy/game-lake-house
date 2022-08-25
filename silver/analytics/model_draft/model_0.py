# Databricks notebook source
from pyspark import pandas as pd

from sklearn import ensemble
from sklearn import model_selection
from sklearn import metrics

# COMMAND ----------

# MAGIC %sql
# MAGIC select distinct patch from bronze_gamelakehouse.dota_match_players
# MAGIC 
# MAGIC where dt_match >= add_months(now(), -6)

# COMMAND ----------

query = '''

select
      match_id,
      max(radiant_win) as radiant_win,
      max(case when hero_id = 1 and isRadiant = true then 1 else 0 end) as radiant_hero_1,
      max(case when hero_id = 2 and isRadiant = true then 1 else 0 end) as radiant_hero_2,
      max(case when hero_id = 3 and isRadiant = true then 1 else 0 end) as radiant_hero_3,
      max(case when hero_id = 4 and isRadiant = true then 1 else 0 end) as radiant_hero_4,
      max(case when hero_id = 5 and isRadiant = true then 1 else 0 end) as radiant_hero_5,
      max(case when hero_id = 6 and isRadiant = true then 1 else 0 end) as radiant_hero_6,
      max(case when hero_id = 7 and isRadiant = true then 1 else 0 end) as radiant_hero_7,
      max(case when hero_id = 8 and isRadiant = true then 1 else 0 end) as radiant_hero_8,
      max(case when hero_id = 9 and isRadiant = true then 1 else 0 end) as radiant_hero_9,
      max(case when hero_id = 10 and isRadiant = true then 1 else 0 end) as radiant_hero_10,
      max(case when hero_id = 11 and isRadiant = true then 1 else 0 end) as radiant_hero_11,
      max(case when hero_id = 12 and isRadiant = true then 1 else 0 end) as radiant_hero_12,
      max(case when hero_id = 13 and isRadiant = true then 1 else 0 end) as radiant_hero_13,
      max(case when hero_id = 14 and isRadiant = true then 1 else 0 end) as radiant_hero_14,
      max(case when hero_id = 15 and isRadiant = true then 1 else 0 end) as radiant_hero_15,
      max(case when hero_id = 16 and isRadiant = true then 1 else 0 end) as radiant_hero_16,
      max(case when hero_id = 17 and isRadiant = true then 1 else 0 end) as radiant_hero_17,
      max(case when hero_id = 18 and isRadiant = true then 1 else 0 end) as radiant_hero_18,
      max(case when hero_id = 19 and isRadiant = true then 1 else 0 end) as radiant_hero_19,
      max(case when hero_id = 20 and isRadiant = true then 1 else 0 end) as radiant_hero_20,
      max(case when hero_id = 21 and isRadiant = true then 1 else 0 end) as radiant_hero_21,
      max(case when hero_id = 22 and isRadiant = true then 1 else 0 end) as radiant_hero_22,
      max(case when hero_id = 23 and isRadiant = true then 1 else 0 end) as radiant_hero_23,
      max(case when hero_id = 25 and isRadiant = true then 1 else 0 end) as radiant_hero_25,
      max(case when hero_id = 26 and isRadiant = true then 1 else 0 end) as radiant_hero_26,
      max(case when hero_id = 27 and isRadiant = true then 1 else 0 end) as radiant_hero_27,
      max(case when hero_id = 28 and isRadiant = true then 1 else 0 end) as radiant_hero_28,
      max(case when hero_id = 29 and isRadiant = true then 1 else 0 end) as radiant_hero_29,
      max(case when hero_id = 30 and isRadiant = true then 1 else 0 end) as radiant_hero_30,
      max(case when hero_id = 31 and isRadiant = true then 1 else 0 end) as radiant_hero_31,
      max(case when hero_id = 32 and isRadiant = true then 1 else 0 end) as radiant_hero_32,
      max(case when hero_id = 33 and isRadiant = true then 1 else 0 end) as radiant_hero_33,
      max(case when hero_id = 34 and isRadiant = true then 1 else 0 end) as radiant_hero_34,
      max(case when hero_id = 35 and isRadiant = true then 1 else 0 end) as radiant_hero_35,
      max(case when hero_id = 36 and isRadiant = true then 1 else 0 end) as radiant_hero_36,
      max(case when hero_id = 37 and isRadiant = true then 1 else 0 end) as radiant_hero_37,
      max(case when hero_id = 38 and isRadiant = true then 1 else 0 end) as radiant_hero_38,
      max(case when hero_id = 39 and isRadiant = true then 1 else 0 end) as radiant_hero_39,
      max(case when hero_id = 40 and isRadiant = true then 1 else 0 end) as radiant_hero_40,
      max(case when hero_id = 41 and isRadiant = true then 1 else 0 end) as radiant_hero_41,
      max(case when hero_id = 42 and isRadiant = true then 1 else 0 end) as radiant_hero_42,
      max(case when hero_id = 43 and isRadiant = true then 1 else 0 end) as radiant_hero_43,
      max(case when hero_id = 44 and isRadiant = true then 1 else 0 end) as radiant_hero_44,
      max(case when hero_id = 45 and isRadiant = true then 1 else 0 end) as radiant_hero_45,
      max(case when hero_id = 46 and isRadiant = true then 1 else 0 end) as radiant_hero_46,
      max(case when hero_id = 47 and isRadiant = true then 1 else 0 end) as radiant_hero_47,
      max(case when hero_id = 48 and isRadiant = true then 1 else 0 end) as radiant_hero_48,
      max(case when hero_id = 49 and isRadiant = true then 1 else 0 end) as radiant_hero_49,
      max(case when hero_id = 50 and isRadiant = true then 1 else 0 end) as radiant_hero_50,
      max(case when hero_id = 51 and isRadiant = true then 1 else 0 end) as radiant_hero_51,
      max(case when hero_id = 52 and isRadiant = true then 1 else 0 end) as radiant_hero_52,
      max(case when hero_id = 53 and isRadiant = true then 1 else 0 end) as radiant_hero_53,
      max(case when hero_id = 54 and isRadiant = true then 1 else 0 end) as radiant_hero_54,
      max(case when hero_id = 55 and isRadiant = true then 1 else 0 end) as radiant_hero_55,
      max(case when hero_id = 56 and isRadiant = true then 1 else 0 end) as radiant_hero_56,
      max(case when hero_id = 57 and isRadiant = true then 1 else 0 end) as radiant_hero_57,
      max(case when hero_id = 58 and isRadiant = true then 1 else 0 end) as radiant_hero_58,
      max(case when hero_id = 59 and isRadiant = true then 1 else 0 end) as radiant_hero_59,
      max(case when hero_id = 60 and isRadiant = true then 1 else 0 end) as radiant_hero_60,
      max(case when hero_id = 61 and isRadiant = true then 1 else 0 end) as radiant_hero_61,
      max(case when hero_id = 62 and isRadiant = true then 1 else 0 end) as radiant_hero_62,
      max(case when hero_id = 63 and isRadiant = true then 1 else 0 end) as radiant_hero_63,
      max(case when hero_id = 64 and isRadiant = true then 1 else 0 end) as radiant_hero_64,
      max(case when hero_id = 65 and isRadiant = true then 1 else 0 end) as radiant_hero_65,
      max(case when hero_id = 66 and isRadiant = true then 1 else 0 end) as radiant_hero_66,
      max(case when hero_id = 67 and isRadiant = true then 1 else 0 end) as radiant_hero_67,
      max(case when hero_id = 68 and isRadiant = true then 1 else 0 end) as radiant_hero_68,
      max(case when hero_id = 69 and isRadiant = true then 1 else 0 end) as radiant_hero_69,
      max(case when hero_id = 70 and isRadiant = true then 1 else 0 end) as radiant_hero_70,
      max(case when hero_id = 71 and isRadiant = true then 1 else 0 end) as radiant_hero_71,
      max(case when hero_id = 72 and isRadiant = true then 1 else 0 end) as radiant_hero_72,
      max(case when hero_id = 73 and isRadiant = true then 1 else 0 end) as radiant_hero_73,
      max(case when hero_id = 74 and isRadiant = true then 1 else 0 end) as radiant_hero_74,
      max(case when hero_id = 75 and isRadiant = true then 1 else 0 end) as radiant_hero_75,
      max(case when hero_id = 76 and isRadiant = true then 1 else 0 end) as radiant_hero_76,
      max(case when hero_id = 77 and isRadiant = true then 1 else 0 end) as radiant_hero_77,
      max(case when hero_id = 78 and isRadiant = true then 1 else 0 end) as radiant_hero_78,
      max(case when hero_id = 79 and isRadiant = true then 1 else 0 end) as radiant_hero_79,
      max(case when hero_id = 80 and isRadiant = true then 1 else 0 end) as radiant_hero_80,
      max(case when hero_id = 81 and isRadiant = true then 1 else 0 end) as radiant_hero_81,
      max(case when hero_id = 82 and isRadiant = true then 1 else 0 end) as radiant_hero_82,
      max(case when hero_id = 83 and isRadiant = true then 1 else 0 end) as radiant_hero_83,
      max(case when hero_id = 84 and isRadiant = true then 1 else 0 end) as radiant_hero_84,
      max(case when hero_id = 85 and isRadiant = true then 1 else 0 end) as radiant_hero_85,
      max(case when hero_id = 86 and isRadiant = true then 1 else 0 end) as radiant_hero_86,
      max(case when hero_id = 87 and isRadiant = true then 1 else 0 end) as radiant_hero_87,
      max(case when hero_id = 88 and isRadiant = true then 1 else 0 end) as radiant_hero_88,
      max(case when hero_id = 89 and isRadiant = true then 1 else 0 end) as radiant_hero_89,
      max(case when hero_id = 90 and isRadiant = true then 1 else 0 end) as radiant_hero_90,
      max(case when hero_id = 91 and isRadiant = true then 1 else 0 end) as radiant_hero_91,
      max(case when hero_id = 92 and isRadiant = true then 1 else 0 end) as radiant_hero_92,
      max(case when hero_id = 93 and isRadiant = true then 1 else 0 end) as radiant_hero_93,
      max(case when hero_id = 94 and isRadiant = true then 1 else 0 end) as radiant_hero_94,
      max(case when hero_id = 95 and isRadiant = true then 1 else 0 end) as radiant_hero_95,
      max(case when hero_id = 96 and isRadiant = true then 1 else 0 end) as radiant_hero_96,
      max(case when hero_id = 97 and isRadiant = true then 1 else 0 end) as radiant_hero_97,
      max(case when hero_id = 98 and isRadiant = true then 1 else 0 end) as radiant_hero_98,
      max(case when hero_id = 99 and isRadiant = true then 1 else 0 end) as radiant_hero_99,
      max(case when hero_id = 100 and isRadiant = true then 1 else 0 end) as radiant_hero_100,
      max(case when hero_id = 101 and isRadiant = true then 1 else 0 end) as radiant_hero_101,
      max(case when hero_id = 102 and isRadiant = true then 1 else 0 end) as radiant_hero_102,
      max(case when hero_id = 103 and isRadiant = true then 1 else 0 end) as radiant_hero_103,
      max(case when hero_id = 104 and isRadiant = true then 1 else 0 end) as radiant_hero_104,
      max(case when hero_id = 105 and isRadiant = true then 1 else 0 end) as radiant_hero_105,
      max(case when hero_id = 106 and isRadiant = true then 1 else 0 end) as radiant_hero_106,
      max(case when hero_id = 107 and isRadiant = true then 1 else 0 end) as radiant_hero_107,
      max(case when hero_id = 108 and isRadiant = true then 1 else 0 end) as radiant_hero_108,
      max(case when hero_id = 109 and isRadiant = true then 1 else 0 end) as radiant_hero_109,
      max(case when hero_id = 110 and isRadiant = true then 1 else 0 end) as radiant_hero_110,
      max(case when hero_id = 111 and isRadiant = true then 1 else 0 end) as radiant_hero_111,
      max(case when hero_id = 112 and isRadiant = true then 1 else 0 end) as radiant_hero_112,
      max(case when hero_id = 113 and isRadiant = true then 1 else 0 end) as radiant_hero_113,
      max(case when hero_id = 114 and isRadiant = true then 1 else 0 end) as radiant_hero_114,
      max(case when hero_id = 119 and isRadiant = true then 1 else 0 end) as radiant_hero_119,
      max(case when hero_id = 120 and isRadiant = true then 1 else 0 end) as radiant_hero_120,
      max(case when hero_id = 121 and isRadiant = true then 1 else 0 end) as radiant_hero_121,
      max(case when hero_id = 123 and isRadiant = true then 1 else 0 end) as radiant_hero_123,
      max(case when hero_id = 126 and isRadiant = true then 1 else 0 end) as radiant_hero_126,
      max(case when hero_id = 128 and isRadiant = true then 1 else 0 end) as radiant_hero_128,
      max(case when hero_id = 129 and isRadiant = true then 1 else 0 end) as radiant_hero_129,
      max(case when hero_id = 135 and isRadiant = true then 1 else 0 end) as radiant_hero_135,
      max(case when hero_id = 136 and isRadiant = true then 1 else 0 end) as radiant_hero_136,
      max(case when hero_id = 137 and isRadiant = true then 1 else 0 end) as radiant_hero_137,
      max(case when hero_id = 1 and isRadiant = false then 1 else 0 end) as dire_hero_1,
      max(case when hero_id = 2 and isRadiant = false then 1 else 0 end) as dire_hero_2,
      max(case when hero_id = 3 and isRadiant = false then 1 else 0 end) as dire_hero_3,
      max(case when hero_id = 4 and isRadiant = false then 1 else 0 end) as dire_hero_4,
      max(case when hero_id = 5 and isRadiant = false then 1 else 0 end) as dire_hero_5,
      max(case when hero_id = 6 and isRadiant = false then 1 else 0 end) as dire_hero_6,
      max(case when hero_id = 7 and isRadiant = false then 1 else 0 end) as dire_hero_7,
      max(case when hero_id = 8 and isRadiant = false then 1 else 0 end) as dire_hero_8,
      max(case when hero_id = 9 and isRadiant = false then 1 else 0 end) as dire_hero_9,
      max(case when hero_id = 10 and isRadiant = false then 1 else 0 end) as dire_hero_10,
      max(case when hero_id = 11 and isRadiant = false then 1 else 0 end) as dire_hero_11,
      max(case when hero_id = 12 and isRadiant = false then 1 else 0 end) as dire_hero_12,
      max(case when hero_id = 13 and isRadiant = false then 1 else 0 end) as dire_hero_13,
      max(case when hero_id = 14 and isRadiant = false then 1 else 0 end) as dire_hero_14,
      max(case when hero_id = 15 and isRadiant = false then 1 else 0 end) as dire_hero_15,
      max(case when hero_id = 16 and isRadiant = false then 1 else 0 end) as dire_hero_16,
      max(case when hero_id = 17 and isRadiant = false then 1 else 0 end) as dire_hero_17,
      max(case when hero_id = 18 and isRadiant = false then 1 else 0 end) as dire_hero_18,
      max(case when hero_id = 19 and isRadiant = false then 1 else 0 end) as dire_hero_19,
      max(case when hero_id = 20 and isRadiant = false then 1 else 0 end) as dire_hero_20,
      max(case when hero_id = 21 and isRadiant = false then 1 else 0 end) as dire_hero_21,
      max(case when hero_id = 22 and isRadiant = false then 1 else 0 end) as dire_hero_22,
      max(case when hero_id = 23 and isRadiant = false then 1 else 0 end) as dire_hero_23,
      max(case when hero_id = 25 and isRadiant = false then 1 else 0 end) as dire_hero_25,
      max(case when hero_id = 26 and isRadiant = false then 1 else 0 end) as dire_hero_26,
      max(case when hero_id = 27 and isRadiant = false then 1 else 0 end) as dire_hero_27,
      max(case when hero_id = 28 and isRadiant = false then 1 else 0 end) as dire_hero_28,
      max(case when hero_id = 29 and isRadiant = false then 1 else 0 end) as dire_hero_29,
      max(case when hero_id = 30 and isRadiant = false then 1 else 0 end) as dire_hero_30,
      max(case when hero_id = 31 and isRadiant = false then 1 else 0 end) as dire_hero_31,
      max(case when hero_id = 32 and isRadiant = false then 1 else 0 end) as dire_hero_32,
      max(case when hero_id = 33 and isRadiant = false then 1 else 0 end) as dire_hero_33,
      max(case when hero_id = 34 and isRadiant = false then 1 else 0 end) as dire_hero_34,
      max(case when hero_id = 35 and isRadiant = false then 1 else 0 end) as dire_hero_35,
      max(case when hero_id = 36 and isRadiant = false then 1 else 0 end) as dire_hero_36,
      max(case when hero_id = 37 and isRadiant = false then 1 else 0 end) as dire_hero_37,
      max(case when hero_id = 38 and isRadiant = false then 1 else 0 end) as dire_hero_38,
      max(case when hero_id = 39 and isRadiant = false then 1 else 0 end) as dire_hero_39,
      max(case when hero_id = 40 and isRadiant = false then 1 else 0 end) as dire_hero_40,
      max(case when hero_id = 41 and isRadiant = false then 1 else 0 end) as dire_hero_41,
      max(case when hero_id = 42 and isRadiant = false then 1 else 0 end) as dire_hero_42,
      max(case when hero_id = 43 and isRadiant = false then 1 else 0 end) as dire_hero_43,
      max(case when hero_id = 44 and isRadiant = false then 1 else 0 end) as dire_hero_44,
      max(case when hero_id = 45 and isRadiant = false then 1 else 0 end) as dire_hero_45,
      max(case when hero_id = 46 and isRadiant = false then 1 else 0 end) as dire_hero_46,
      max(case when hero_id = 47 and isRadiant = false then 1 else 0 end) as dire_hero_47,
      max(case when hero_id = 48 and isRadiant = false then 1 else 0 end) as dire_hero_48,
      max(case when hero_id = 49 and isRadiant = false then 1 else 0 end) as dire_hero_49,
      max(case when hero_id = 50 and isRadiant = false then 1 else 0 end) as dire_hero_50,
      max(case when hero_id = 51 and isRadiant = false then 1 else 0 end) as dire_hero_51,
      max(case when hero_id = 52 and isRadiant = false then 1 else 0 end) as dire_hero_52,
      max(case when hero_id = 53 and isRadiant = false then 1 else 0 end) as dire_hero_53,
      max(case when hero_id = 54 and isRadiant = false then 1 else 0 end) as dire_hero_54,
      max(case when hero_id = 55 and isRadiant = false then 1 else 0 end) as dire_hero_55,
      max(case when hero_id = 56 and isRadiant = false then 1 else 0 end) as dire_hero_56,
      max(case when hero_id = 57 and isRadiant = false then 1 else 0 end) as dire_hero_57,
      max(case when hero_id = 58 and isRadiant = false then 1 else 0 end) as dire_hero_58,
      max(case when hero_id = 59 and isRadiant = false then 1 else 0 end) as dire_hero_59,
      max(case when hero_id = 60 and isRadiant = false then 1 else 0 end) as dire_hero_60,
      max(case when hero_id = 61 and isRadiant = false then 1 else 0 end) as dire_hero_61,
      max(case when hero_id = 62 and isRadiant = false then 1 else 0 end) as dire_hero_62,
      max(case when hero_id = 63 and isRadiant = false then 1 else 0 end) as dire_hero_63,
      max(case when hero_id = 64 and isRadiant = false then 1 else 0 end) as dire_hero_64,
      max(case when hero_id = 65 and isRadiant = false then 1 else 0 end) as dire_hero_65,
      max(case when hero_id = 66 and isRadiant = false then 1 else 0 end) as dire_hero_66,
      max(case when hero_id = 67 and isRadiant = false then 1 else 0 end) as dire_hero_67,
      max(case when hero_id = 68 and isRadiant = false then 1 else 0 end) as dire_hero_68,
      max(case when hero_id = 69 and isRadiant = false then 1 else 0 end) as dire_hero_69,
      max(case when hero_id = 70 and isRadiant = false then 1 else 0 end) as dire_hero_70,
      max(case when hero_id = 71 and isRadiant = false then 1 else 0 end) as dire_hero_71,
      max(case when hero_id = 72 and isRadiant = false then 1 else 0 end) as dire_hero_72,
      max(case when hero_id = 73 and isRadiant = false then 1 else 0 end) as dire_hero_73,
      max(case when hero_id = 74 and isRadiant = false then 1 else 0 end) as dire_hero_74,
      max(case when hero_id = 75 and isRadiant = false then 1 else 0 end) as dire_hero_75,
      max(case when hero_id = 76 and isRadiant = false then 1 else 0 end) as dire_hero_76,
      max(case when hero_id = 77 and isRadiant = false then 1 else 0 end) as dire_hero_77,
      max(case when hero_id = 78 and isRadiant = false then 1 else 0 end) as dire_hero_78,
      max(case when hero_id = 79 and isRadiant = false then 1 else 0 end) as dire_hero_79,
      max(case when hero_id = 80 and isRadiant = false then 1 else 0 end) as dire_hero_80,
      max(case when hero_id = 81 and isRadiant = false then 1 else 0 end) as dire_hero_81,
      max(case when hero_id = 82 and isRadiant = false then 1 else 0 end) as dire_hero_82,
      max(case when hero_id = 83 and isRadiant = false then 1 else 0 end) as dire_hero_83,
      max(case when hero_id = 84 and isRadiant = false then 1 else 0 end) as dire_hero_84,
      max(case when hero_id = 85 and isRadiant = false then 1 else 0 end) as dire_hero_85,
      max(case when hero_id = 86 and isRadiant = false then 1 else 0 end) as dire_hero_86,
      max(case when hero_id = 87 and isRadiant = false then 1 else 0 end) as dire_hero_87,
      max(case when hero_id = 88 and isRadiant = false then 1 else 0 end) as dire_hero_88,
      max(case when hero_id = 89 and isRadiant = false then 1 else 0 end) as dire_hero_89,
      max(case when hero_id = 90 and isRadiant = false then 1 else 0 end) as dire_hero_90,
      max(case when hero_id = 91 and isRadiant = false then 1 else 0 end) as dire_hero_91,
      max(case when hero_id = 92 and isRadiant = false then 1 else 0 end) as dire_hero_92,
      max(case when hero_id = 93 and isRadiant = false then 1 else 0 end) as dire_hero_93,
      max(case when hero_id = 94 and isRadiant = false then 1 else 0 end) as dire_hero_94,
      max(case when hero_id = 95 and isRadiant = false then 1 else 0 end) as dire_hero_95,
      max(case when hero_id = 96 and isRadiant = false then 1 else 0 end) as dire_hero_96,
      max(case when hero_id = 97 and isRadiant = false then 1 else 0 end) as dire_hero_97,
      max(case when hero_id = 98 and isRadiant = false then 1 else 0 end) as dire_hero_98,
      max(case when hero_id = 99 and isRadiant = false then 1 else 0 end) as dire_hero_99,
      max(case when hero_id = 100 and isRadiant = false then 1 else 0 end) as dire_hero_100,
      max(case when hero_id = 101 and isRadiant = false then 1 else 0 end) as dire_hero_101,
      max(case when hero_id = 102 and isRadiant = false then 1 else 0 end) as dire_hero_102,
      max(case when hero_id = 103 and isRadiant = false then 1 else 0 end) as dire_hero_103,
      max(case when hero_id = 104 and isRadiant = false then 1 else 0 end) as dire_hero_104,
      max(case when hero_id = 105 and isRadiant = false then 1 else 0 end) as dire_hero_105,
      max(case when hero_id = 106 and isRadiant = false then 1 else 0 end) as dire_hero_106,
      max(case when hero_id = 107 and isRadiant = false then 1 else 0 end) as dire_hero_107,
      max(case when hero_id = 108 and isRadiant = false then 1 else 0 end) as dire_hero_108,
      max(case when hero_id = 109 and isRadiant = false then 1 else 0 end) as dire_hero_109,
      max(case when hero_id = 110 and isRadiant = false then 1 else 0 end) as dire_hero_110,
      max(case when hero_id = 111 and isRadiant = false then 1 else 0 end) as dire_hero_111,
      max(case when hero_id = 112 and isRadiant = false then 1 else 0 end) as dire_hero_112,
      max(case when hero_id = 113 and isRadiant = false then 1 else 0 end) as dire_hero_113,
      max(case when hero_id = 114 and isRadiant = false then 1 else 0 end) as dire_hero_114,
      max(case when hero_id = 119 and isRadiant = false then 1 else 0 end) as dire_hero_119,
      max(case when hero_id = 120 and isRadiant = false then 1 else 0 end) as dire_hero_120,
      max(case when hero_id = 121 and isRadiant = false then 1 else 0 end) as dire_hero_121,
      max(case when hero_id = 123 and isRadiant = false then 1 else 0 end) as dire_hero_123,
      max(case when hero_id = 126 and isRadiant = false then 1 else 0 end) as dire_hero_126,
      max(case when hero_id = 128 and isRadiant = false then 1 else 0 end) as dire_hero_128,
      max(case when hero_id = 129 and isRadiant = false then 1 else 0 end) as dire_hero_129,
      max(case when hero_id = 135 and isRadiant = false then 1 else 0 end) as dire_hero_135,
      max(case when hero_id = 136 and isRadiant = false then 1 else 0 end) as dire_hero_136,
      max(case when hero_id = 137 and isRadiant = false then 1 else 0 end) as dire_hero_137

from bronze_gamelakehouse.dota_match_players

where 1=1
-- and dt_match >= add_months(now(), -6)
and patch = 50

group by match_id'''

df = spark.sql(query).toPandas()

# COMMAND ----------

target_column = 'radiant_win'
id_column = 'match_id'
features_column = list(set(df.columns.tolist()) - set( [target_column, id_column] ))

# COMMAND ----------

X_train, X_test, y_train, y_test = model_selection.train_test_split(df[features_column], df[target_column],
                                                                     test_size=0.2, random_state=42)

# COMMAND ----------

model = ensemble.RandomForestClassifier(n_jobs=-1)

grid_params = {"min_samples_leaf": [10,20,30,50],
               "n_estimators":[100,200,300,500],
               "max_depth":[5,8,15]
              }

grid_search = model_selection.GridSearchCV(model, grid_params, cv=3, scoring='roc_auc', verbose=3)

grid_search.fit(X_train, y_train)

# COMMAND ----------

y_train_predict = grid_search.predict(X_train)
y_train_proba = grid_search.predict_proba(X_train)

acc_train = metrics.accuracy_score(y_train, y_train_predict)
auc_train = metrics.roc_auc_score(y_train, y_train_proba[:,1])

print("Acc Train:", acc_train)
print("AUC Train:", auc_train)

# COMMAND ----------

y_test_predict = grid_search.predict(X_test)
y_test_proba = grid_search.predict_proba(X_test)

acc_test = metrics.accuracy_score(y_test, y_test_predict)
auc_test = metrics.roc_auc_score(y_test, y_test_proba[:,1])

print("Acc Train:", acc_test)
print("AUC Train:", auc_test)

# COMMAND ----------

df_results = pd.DataFrame(grid_search.cv_results_)
df_results.sort_values(by='rank_test_score')

# COMMAND ----------


