
create table commodities (
	day         TEXT, 
	name        TEXT, 
	price       REAL, 
	open_price  REAL, 
	high_price  REAL, 
	low_price   REAL, 
	volume      INTEGER, 
	change      REAL
);

insert into commodities values ('31/07/2015', 'Brent Oil', 52.85, 53.84, 54.12, 52.24, 121510, -2.09);
insert into commodities values ('30/07/2015', 'Brent Oil', 53.98, 54.22, 55.00, 53.71, 125940, -0.11);
insert into commodities values ('29/07/2015', 'Brent Oil', 54.04, 53.55, 54.96, 53.12, 171520, 0.33);
insert into commodities values ('28/07/2015', 'Brent Oil', 53.86, 53.38, 54.60, 52.79, 156720, -0.13);
insert into commodities values ('27/07/2015', 'Brent Oil', 53.93, 54.91, 55.28, 53.29, 128180, -1.98);
insert into commodities values ('24/07/2015', 'Brent Oil', 55.02, 55.76, 55.96, 54.70, 101460, -1.13);
insert into commodities values ('23/07/2015', 'Brent Oil', 55.65, 56.48, 56.88, 55.50, 92250, -1.49);
insert into commodities values ('22/07/2015', 'Brent Oil', 56.49, 57.31, 57.31, 56.28, 105900, -1.67);
insert into commodities values ('21/07/2015', 'Brent Oil', 57.45, 56.70, 57.80, 56.70, 84320, 0.79);
insert into commodities values ('20/07/2015', 'Brent Oil', 57.00, 57.37, 57.78, 56.69, 62430, -0.18);
insert into commodities values ('17/07/2015', 'Brent Oil', 57.10, 56.93, 57.35, 56.40, 157130, 0.32);
insert into commodities values ('16/07/2015', 'Brent Oil', 56.92, 57.36, 58.00, 56.86, 246840, -0.35);
insert into commodities values ('15/07/2015', 'Brent Oil', 57.12, 58.80, 58.98, 56.92, 274670, -2.66);
insert into commodities values ('14/07/2015', 'Brent Oil', 58.68, 58.05, 58.98, 56.75, 330180, 0.91);
insert into commodities values ('13/07/2015', 'Brent Oil', 58.15, 58.59, 59.14, 57.16, 216370, -1.44);
insert into commodities values ('10/07/2015', 'Brent Oil', 59.00, 58.87, 59.90, 58.15, 197340, 0.20);
insert into commodities values ('09/07/2015', 'Brent Oil', 58.88, 57.60, 59.50, 57.31, 264470, 2.60);
insert into commodities values ('08/07/2015', 'Brent Oil', 57.39, 57.75, 58.15, 56.35, 264130, 0.05);
insert into commodities values ('07/07/2015', 'Brent Oil', 57.36, 57.28, 58.31, 55.60, 230110, 0.58);
insert into commodities values ('06/07/2015', 'Brent Oil', 57.03, 60.45, 60.54, 56.87, 172230, -6.26);
insert into commodities values ('03/07/2015', 'Brent Oil', 60.84, 62.28, 62.56, 60.63, 63800, -2.76);
insert into commodities values ('02/07/2015', 'Brent Oil', 62.57, 62.70, 63.65, 62.27, 149010, 0.16);
insert into commodities values ('01/07/2015', 'Brent Oil', 62.47, 63.60, 63.85, 62.27, 200670, -2.60);

insert into commodities values ('31/07/2015', 'Gas Oil', 492.50, 495.50, 497.00, 485.50, 75450, -1.01);
insert into commodities values ('30/07/2015', 'Gas Oil', 497.50, 493.25, 502.00, 492.75, 58860, -0.40);
insert into commodities values ('29/07/2015', 'Gas Oil', 499.50, 491.00, 500.50, 487.00, 75730, 0.10);
insert into commodities values ('28/07/2015', 'Gas Oil', 499.00, 488.25, 501.00, 484.00, 98310, 0.96);
insert into commodities values ('27/07/2015', 'Gas Oil', 494.25, 503.25, 504.00, 488.00, 57630, -1.98);
insert into commodities values ('24/07/2015', 'Gas Oil', 504.25, 510.75, 511.25, 500.75, 56210, -1.94);
insert into commodities values ('23/07/2015', 'Gas Oil', 514.25, 514.00, 517.25, 506.75, 59070, -0.82);
insert into commodities values ('22/07/2015', 'Gas Oil', 518.50, 516.75, 519.50, 513.00, 60840, 0.53);
insert into commodities values ('21/07/2015', 'Gas Oil', 515.75, 512.25, 520.75, 510.75, 49790, 0.00);
insert into commodities values ('20/07/2015', 'Gas Oil', 515.75, 516.75, 519.50, 511.50, 40060, 0.78);
insert into commodities values ('17/07/2015', 'Gas Oil', 511.75, 516.00, 519.25, 510.00, 51360, -1.54);
insert into commodities values ('16/07/2015', 'Gas Oil', 519.75, 519.00, 522.50, 515.00, 61370, -0.72);
insert into commodities values ('15/07/2015', 'Gas Oil', 523.50, 533.50, 536.75, 516.50, 75550, -1.09);
insert into commodities values ('14/07/2015', 'Gas Oil', 529.25, 532.50, 536.75, 522.75, 84120, -1.40);
insert into commodities values ('13/07/2015', 'Gas Oil', 536.75, 537.75, 541.25, 525.00, 69010, 0.42);
insert into commodities values ('10/07/2015', 'Gas Oil', 534.50, 536.75, 545.50, 532.75, 76850, -0.65);
insert into commodities values ('09/07/2015', 'Gas Oil', 538.00, 530.25, 543.50, 529.75, 114270, 2.28);
insert into commodities values ('08/07/2015', 'Gas Oil', 526.00, 531.50, 534.75, 520.50, 131840, 1.06);
insert into commodities values ('07/07/2015', 'Gas Oil', 520.50, 530.75, 535.75, 515.75, 118260, -3.39);
insert into commodities values ('06/07/2015', 'Gas Oil', 538.75, 558.50, 566.75, 526.00, 112080, -3.23);
insert into commodities values ('03/07/2015', 'Gas Oil', 556.75, 567.50, 569.00, 554.75, 47120, -3.38);
insert into commodities values ('02/07/2015', 'Gas Oil', 576.25, 568.00, 579.00, 565.50, 73160, 0.30);
insert into commodities values ('01/07/2015', 'Gas Oil', 574.50, 574.25, 580.50, 566.25, 73190, -0.26);

insert into commodities values ('31/07/2015', 'Natural Gas', 2716.00, 2782.00, 2788.00, 2706.00, 117370, -1.88);
insert into commodities values ('30/07/2015', 'Natural Gas', 2768.00, 2858.00, 2895.00, 2762.00, 146550, -3.35);
insert into commodities values ('29/07/2015', 'Natural Gas', 2864.00, 2827.00, 2870.00, 2821.00, 119130, 1.7);
insert into commodities values ('28/07/2015', 'Natural Gas', 2816.00, 2794.00, 2847.00, 2775.00, 125870, 1);
insert into commodities values ('27/07/2015', 'Natural Gas', 2788.00, 2745.00, 2817.00, 2735.00, 77980, 0.47);
insert into commodities values ('24/07/2015', 'Natural Gas', 2775.00, 2811.00, 2818.00, 2772.00, 73560, -1.49);
insert into commodities values ('23/07/2015', 'Natural Gas', 2817.00, 2905.00, 2957.00, 2806.00, 147370, -3.13);
insert into commodities values ('22/07/2015', 'Natural Gas', 2908.00, 2897.00, 2921.00, 2851.00, 62960, 0.62);
insert into commodities values ('21/07/2015', 'Natural Gas', 2890.00, 2842.00, 2898.00, 2830.00, 59540, 2.12);
insert into commodities values ('20/07/2015', 'Natural Gas', 2830.00, 2849.00, 2883.00, 2794.00, 75350, -1.53);
insert into commodities values ('17/07/2015', 'Natural Gas', 2874.00, 2861.00, 2893.00, 2828.00, 49420, 0.74);
insert into commodities values ('16/07/2015', 'Natural Gas', 2853.00, 2905.00, 2921.00, 2843.00, 80790, -1.93);
insert into commodities values ('15/07/2015', 'Natural Gas', 2909.00, 2842.00, 2914.00, 2831.00, 91640, 2.47);
insert into commodities values ('14/07/2015', 'Natural Gas', 2839.00, 2870.00, 2931.00, 2823.00, 73870, -0.98);
insert into commodities values ('13/07/2015', 'Natural Gas', 2867.00, 2821.00, 2884.00, 2794.00, 97480, 3.13);
insert into commodities values ('10/07/2015', 'Natural Gas', 2780.00, 2748.00, 2815.00, 2735.00, 60000, 1.53);
insert into commodities values ('09/07/2015', 'Natural Gas', 2738.00, 2699.00, 2748.00, 2656.00, 68930, 1.52);
insert into commodities values ('08/07/2015', 'Natural Gas', 2697.00, 2733.00, 2766.00, 2687.00, 76180, -1.06);
insert into commodities values ('07/07/2015', 'Natural Gas', 2726.00, 2767.00, 2809.00, 2701.00, 41980, -1.45);
insert into commodities values ('06/07/2015', 'Natural Gas', 2766.00, 2850.00, 2871.00, 2745.00, 47070, -0.36);
insert into commodities values ('03/07/2015', 'Natural Gas', 2776.00, 2839.00, 2855.00, 2763.00, 0, -1.63);
insert into commodities values ('02/07/2015', 'Natural Gas', 2822.00, 2798.00, 2885.00, 2795.00, 156310, 1.4);
insert into commodities values ('01/07/2015', 'Natural Gas', 2783.00, 2816.00, 2856.00, 2761.00, 124690, -1.73);

insert into commodities values ('31/07/2015', 'Gold', 1095.10, 1087.70, 1103.00, 1079.20, 174130, 0.59);
insert into commodities values ('30/07/2015', 'Gold', 1088.70, 1096.60, 1098.20, 1081.50, 169100, -0.42);
insert into commodities values ('29/07/2015', 'Gold', 1093.30, 1094.80, 1101.50, 1089.80, 113570, -0.31);
insert into commodities values ('28/07/2015', 'Gold', 1096.70, 1094.20, 1098.70, 1091.10, 78100, -0.02);
insert into commodities values ('27/07/2015', 'Gold', 1096.90, 1098.20, 1104.90, 1088.00, 53580, 1.00);
insert into commodities values ('24/07/2015', 'Gold', 1086.00, 1090.30, 1101.50, 1073.70, 55300, -0.83);
insert into commodities values ('23/07/2015', 'Gold', 1095.10, 1094.50, 1105.80, 1087.10, 61880, 0.23);
insert into commodities values ('22/07/2015', 'Gold', 1092.60, 1102.80, 1103.30, 1087.00, 26720, -1.09);
insert into commodities values ('21/07/2015', 'Gold', 1104.60, 1098.80, 1110.00, 1097.50, 37770, -0.32);
insert into commodities values ('20/07/2015', 'Gold', 1108.20, 1133.80, 1133.80, 1083.50, 32920, -2.23);
insert into commodities values ('17/07/2015', 'Gold', 1133.50, 1145.60, 1146.30, 1131.30, 24020, -1.09);
insert into commodities values ('16/07/2015', 'Gold', 1146.00, 1149.90, 1149.90, 1142.70, 30530, -0.31);
insert into commodities values ('15/07/2015', 'Gold', 1149.60, 1157.50, 1157.90, 1144.00, 16580, -0.53);
insert into commodities values ('14/07/2015', 'Gold', 1155.70, 1159.30, 1161.00, 1154.40, 27430, -0.17);
insert into commodities values ('13/07/2015', 'Gold', 1157.70, 1164.50, 1165.40, 1152.10, 17980, -0.22);
insert into commodities values ('10/07/2015', 'Gold', 1160.20, 1161.10, 1166.80, 1159.00, 17630, -0.11);
insert into commodities values ('09/07/2015', 'Gold', 1161.50, 1159.60, 1169.00, 1157.30, 17140, -0.37);
insert into commodities values ('08/07/2015', 'Gold', 1165.80, 1156.50, 1166.10, 1148.50, 14520, 0.94);
insert into commodities values ('07/07/2015', 'Gold', 1154.90, 1171.50, 1171.50, 1149.00, 9450, -1.75);
insert into commodities values ('06/07/2015', 'Gold', 1175.50, 1167.20, 1175.70, 1165.00, 5750, 0.66);
insert into commodities values ('03/07/2015', 'Gold', 1167.80, 1164.70, 1169.00, 1164.60, 0, 0.41);
insert into commodities values ('02/07/2015', 'Gold', 1163.00, 1166.40, 1168.10, 1155.50, 320, -0.51);
insert into commodities values ('01/07/2015', 'Gold', 1169.00, 1173.10, 1173.10, 1166.60, 460, -0.21);
