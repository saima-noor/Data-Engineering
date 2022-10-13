
 # Navicat Premium Data Transfer
 #
 # Source Server         : Localhost
 # Source Server Type    : MySQL
 # Source Server Version : 100136
 # Source Host           : localhost:3306
 # Source Schema         : aqm_db
 #
 # Target Server Type    : MySQL
 # Target Server Version : 100136
 # File Encoding         : 65001
 #
 # Date: 16/05/2021 10:21:26


# SET NAMES utf8mb4;
# SET FOREIGN_KEY_CHECKS = 0;

# -- ----------------------------
# -- Table structure for cats_dimension
# -- ----------------------------
CREATE_cats_dimension_TABLE_IF_NOT_EXISTS = """
CREATE TABLE IF NOT EXISTS `cats_dimension`  (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `mtime` timestamp(0) DEFAULT CURRENT_TIMESTAMP(0) ON UPDATE CURRENT_TIMESTAMP(0),
  `parent` int(11) NOT NULL DEFAULT 0,
  `inherit` int(11) NOT NULL DEFAULT 0,
  `slug` varchar(200) CHARACTER SET latin1 COLLATE latin1_swedish_ci NOT NULL,
  `display_label` varchar(50) CHARACTER SET latin1 COLLATE latin1_swedish_ci NULL DEFAULT NULL,
  `partner_tpg` int(6) NULL DEFAULT NULL,
  `flag` int(11) NOT NULL DEFAULT 0,
  `stts` int(11) NOT NULL DEFAULT 0,
  `sorting_order` int(3) NULL DEFAULT NULL,
  PRIMARY KEY (`id`) USING BTREE,
  INDEX `stts`(`stts`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 553 CHARACTER SET = latin1 COLLATE = latin1_swedish_ci ROW_FORMAT = Compact;
"""
# -- ----------------------------
# -- Table structure for locations_dimension
# -- ----------------------------
CREATE_locations_dimension_TABLE_IF_NOT_EXISTS = """
CREATE TABLE IF NOT EXISTS `locations_dimension`  (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `mtime` timestamp(0) DEFAULT CURRENT_TIMESTAMP(0) ON UPDATE CURRENT_TIMESTAMP(0),
  `parent` int(11) NOT NULL DEFAULT 0,
  `ltype` int(11) NOT NULL DEFAULT 0,
  `slug` varchar(200) CHARACTER SET latin1 COLLATE latin1_swedish_ci NOT NULL,
  `stts` int(11) NOT NULL DEFAULT 0,
  PRIMARY KEY (`id`) USING BTREE,
  INDEX `stts`(`stts`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 315 CHARACTER SET = latin1 COLLATE = latin1_swedish_ci ROW_FORMAT = Compact;
"""
# -- ----------------------------
# -- Table structure for company_dimension
# -- ----------------------------
CREATE_company_dimension_TABLE_IF_NOT_EXISTS = """
CREATE TABLE IF NOT EXISTS `company_dimension`  (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `mtime` timestamp(0) NOT NULL DEFAULT CURRENT_TIMESTAMP(0),
  `name` varchar(200) CHARACTER SET latin1 COLLATE latin1_swedish_ci NOT NULL DEFAULT 'NAME',
  `owner` varchar(200) CHARACTER SET latin1 COLLATE latin1_swedish_ci NOT NULL DEFAULT 'OWNER',
  `uname` varchar(50) CHARACTER SET latin1 COLLATE latin1_swedish_ci NOT NULL DEFAULT 'UNAME',
  `pass` varchar(32) CHARACTER SET latin1 COLLATE latin1_swedish_ci NOT NULL DEFAULT 'PASS',
  `email` varchar(200) CHARACTER SET latin1 COLLATE latin1_swedish_ci NOT NULL,
  `address` varchar(255) CHARACTER SET latin1 COLLATE latin1_swedish_ci NOT NULL DEFAULT 'ADDRESS',
  `contact` varchar(200) CHARACTER SET latin1 COLLATE latin1_swedish_ci NOT NULL DEFAULT 'CONTACT',
  `desk` varchar(2000) CHARACTER SET latin1 COLLATE latin1_swedish_ci NOT NULL DEFAULT 'DESK',
  `short_name` varchar(100) CHARACTER SET latin1 COLLATE latin1_swedish_ci NULL DEFAULT NULL,
  `stock_start_date` date NULL DEFAULT '2015-10-27',
  `stts` int(11) NOT NULL,
  `region` int(20) NOT NULL,
  `area` int(20) NOT NULL,
  `updated` timestamp(0) NULL DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP(0),
  `sales_type` enum('BAT','MT','WE') CHARACTER SET latin1 COLLATE latin1_swedish_ci NULL DEFAULT 'BAT',
  `no_of_points` int(10) NULL DEFAULT NULL,
  `image` varchar(200),
  PRIMARY KEY (`id`) USING BTREE,
  UNIQUE INDEX `uname`(`uname`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 70 CHARACTER SET = latin1 COLLATE = latin1_swedish_ci ROW_FORMAT = Compact;
"""
# -- ----------------------------
# -- Table structure for daily_sales_msr_fact
# -- ----------------------------
CREATE_daily_sales_msr_fact_TABLE_IF_NOT_EXISTS = """
CREATE TABLE IF NOT EXISTS `daily_sales_msr_fact`  (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `mtime` timestamp(0) NOT NULL DEFAULT CURRENT_TIMESTAMP(0),
  `dpid` int(11) NOT NULL,
  `route_id` int(11) NOT NULL,
  `skid` int(11) NOT NULL,
  `group` int(11) NOT NULL,
  `family` int(11) NOT NULL,
  `date` date NOT NULL DEFAULT '1990-01-01',
  `sale` double NOT NULL,
  `dprice` double NOT NULL,
  `rprice` double NOT NULL,
  `dcc_price` double NOT NULL,
  `issue` double NOT NULL,
  `return` double NULL DEFAULT NULL,
  `memos` int(11) NOT NULL,
  `vmemos` int(11) NOT NULL,
  `tlp` double NOT NULL,
  `cnc` double NOT NULL,
  `vp` double NOT NULL,
  `mvp` double NOT NULL,
  `p` double NOT NULL,
  `tcc` double NOT NULL,
  `dcc` double NOT NULL,
  `ecnc` double NOT NULL,
  `gt` double NOT NULL,
  `struc` double NOT NULL,
  `semi_struc` double NOT NULL,
  `streetk` double NOT NULL,
  `mass_hrc` double NOT NULL,
  `pop_hrc` double NOT NULL,
  `prem_hrc` double NOT NULL,
  `kaccounts` double NOT NULL,
  `ogrocery` double NOT NULL,
  `snb_cnc` double NULL DEFAULT NULL,
  `pay_n_go` double NULL DEFAULT NULL,
  `shop_n_browse` double NULL DEFAULT NULL,
  `entertainment` double NULL DEFAULT NULL,
  `outlets` int(11) NOT NULL,
  `apps_version` varchar(20) CHARACTER SET latin1 COLLATE latin1_swedish_ci NULL DEFAULT NULL,
  `updated` timestamp(0) NULL DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP(0),
  `visited` int(11) NOT NULL,
  PRIMARY KEY (`id`) USING BTREE,
  INDEX `dp_ind`(`dpid`) USING BTREE,
  INDEX `route_ind`(`route_id`) USING BTREE,
  INDEX `sku_ind`(`skid`) USING BTREE,
  INDEX `group_ind`(`group`) USING BTREE,
  INDEX `family_ind`(`family`) USING BTREE,
  UNIQUE KEY unique_fields (dpid,route_id,skid,date)
) ENGINE = InnoDB CHARACTER SET = utf8 COLLATE = utf8_general_ci ROW_FORMAT = Compact;
"""
# -- ----------------------------
# -- Table structure for staging_daily_sales_msr
# -- ----------------------------
CREATE_staging_daily_sales_msr_TABLE_IF_NOT_EXISTS = """
CREATE TABLE IF NOT EXISTS `staged_daliy_sales_msr_table` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `mtime` timestamp(0) NOT NULL DEFAULT CURRENT_TIMESTAMP(0),
  `dpid` int(11) NOT NULL,
  `route_id` int(11) NOT NULL,
  `skid` int(11) NOT NULL,
  `prid` int(11) NOT NULL,
  `group` int(11) NOT NULL,
  `family` int(11) NOT NULL,
  `date` date NOT NULL DEFAULT '1990-01-01',
  `datetime` timestamp(0) NOT NULL DEFAULT '1990-01-01 00:00:00',
  `sale` double NOT NULL,
  `dprice` double NOT NULL,
  `rprice` double NOT NULL,
  `dcc_price` double NOT NULL,
  `issue` double NOT NULL,
  `return` double NULL DEFAULT NULL,
  `memos` int(11) NOT NULL,
  `vmemos` int(11) NOT NULL,
  `tlp` double NOT NULL,
  `cnc` double NOT NULL,
  `vp` double NOT NULL,
  `mvp` double NOT NULL,
  `p` double NOT NULL,
  `tcc` double NOT NULL,
  `dcc` double NOT NULL,
  `ecnc` double NOT NULL,
  `gt` double NOT NULL,
  `struc` double NOT NULL,
  `semi_struc` double NOT NULL,
  `streetk` double NOT NULL,
  `mass_hrc` double NOT NULL,
  `pop_hrc` double NOT NULL,
  `prem_hrc` double NOT NULL,
  `kaccounts` double NOT NULL,
  `ogrocery` double NOT NULL,
  `snb_cnc` double NULL DEFAULT NULL,
  `pay_n_go` double NULL DEFAULT NULL,
  `shop_n_browse` double NULL DEFAULT NULL,
  `entertainment` double NULL DEFAULT NULL,
  `outlets` int(11) NOT NULL,
  `apps_version` varchar(20) CHARACTER SET latin1 COLLATE latin1_swedish_ci NULL DEFAULT NULL,
  `updated` timestamp(0) NULL DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP(0),
  `visited` int(11) NOT NULL,
  INDEX `dp_ind`(`dpid`) USING BTREE,
  INDEX `route_ind`(`route_id`) USING BTREE,
  INDEX `sku_ind`(`skid`) USING BTREE,
  INDEX `group_ind`(`group`) USING BTREE,
  INDEX `family_ind`(`family`) USING BTREE,
  PRIMARY KEY (`date`, `dpid`, `route_id`, `skid`) USING BTREE
) ENGINE = InnoDB CHARACTER SET = utf8 COLLATE = utf8_general_ci ROW_FORMAT = Compact;
"""
# -- ----------------------------
# -- Table structure for distributorspoint_dimension
# -- ----------------------------
CREATE_distributorspoint_dimension_TABLE_IF_NOT_EXISTS = """
CREATE TABLE IF NOT EXISTS `distributorspoint_dimension`  (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `mtime` timestamp(0) NOT NULL DEFAULT CURRENT_TIMESTAMP(0),
  `dsid` int(11) NOT NULL COMMENT 'dsid = company id',
  `region` int(11) NOT NULL,
  `area` int(11) NOT NULL,
  `territory` int(11) NOT NULL,
  `name` varchar(200) CHARACTER SET latin1 COLLATE latin1_swedish_ci NOT NULL DEFAULT '',
  `email` varchar(200) CHARACTER SET latin1 COLLATE latin1_swedish_ci NOT NULL DEFAULT '',
  `pass` varchar(32) CHARACTER SET latin1 COLLATE latin1_swedish_ci NOT NULL DEFAULT '',
  `contact` varchar(200) CHARACTER SET latin1 COLLATE latin1_swedish_ci NOT NULL DEFAULT '',
  `address` varchar(200) CHARACTER SET latin1 COLLATE latin1_swedish_ci NOT NULL DEFAULT '',
  `skus` varchar(200) CHARACTER SET latin1 COLLATE latin1_swedish_ci NOT NULL DEFAULT '',
  `de_date` date NULL DEFAULT NULL,
  `stock_start_date` date NOT NULL DEFAULT '2015-10-27',
  `pda` int(11) NOT NULL DEFAULT 0,
  `stts` int(11) NOT NULL,
  `updated` timestamp(0) NULL DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP(0),
  `sales_type` enum('BAT','MT') CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT 'BAT',
  `we_point` tinyint(4) NULL DEFAULT 0,
  `we_skus` varchar(200) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL,
  `av_app_status` varchar(200),
  PRIMARY KEY (`id`) USING BTREE,
  INDEX `ds_ind`(`dsid`) USING BTREE,
  INDEX `region_ind`(`region`) USING BTREE,
  INDEX `area_ind`(`area`) USING BTREE,
  INDEX `teritory_ind`(`territory`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 470 CHARACTER SET = utf8 COLLATE = utf8_general_ci ROW_FORMAT = Compact;
"""
# -- ----------------------------
# -- Table structure for products_dimension
# -- ----------------------------
CREATE_products_dimension_TABLE_IF_NOT_EXISTS = """
CREATE TABLE IF NOT EXISTS `products_dimension`  (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `mtime` timestamp(0) NOT NULL DEFAULT CURRENT_TIMESTAMP(0) ON UPDATE CURRENT_TIMESTAMP(0),
  `name` varchar(200) CHARACTER SET latin1 COLLATE latin1_swedish_ci NOT NULL,
  `family` int(11) NOT NULL,
  `group` int(11) NOT NULL,
  `color` varchar(32) CHARACTER SET latin1 COLLATE latin1_swedish_ci NOT NULL,
  `desk` varchar(1200) CHARACTER SET latin1 COLLATE latin1_swedish_ci NOT NULL,
  `flags` int(11) NOT NULL,
  `targetvalue` float NOT NULL,
  `oosr` float NOT NULL,
  `ioq` int(11) NOT NULL,
  `stts` int(11) NOT NULL,
  `sort` float(11, 3) NULL DEFAULT NULL,
  `report_enable` float(11, 3) NULL DEFAULT 1.000,
  `is_new` tinyint(4) NULL DEFAULT 0,
  PRIMARY KEY (`id`) USING BTREE,
  UNIQUE INDEX `name`(`name`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 34 CHARACTER SET = latin1 COLLATE = latin1_swedish_ci ROW_FORMAT = Compact;
"""
# -- ----------------------------
# -- Table structure for routes_dimension
# -- ----------------------------
CREATE_routes_dimension_TABLE_IF_NOT_EXISTS = """
CREATE TABLE IF NOT EXISTS `routes_dimension`  (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `mtime` timestamp(0) DEFAULT CURRENT_TIMESTAMP(0),
  `number` int(11) NOT NULL DEFAULT 1,
  `section` int(11) NOT NULL DEFAULT 498,
  `dsid` int(11) NOT NULL,
  `dpid` int(11) NOT NULL,
  `tsa_id` int(10) NULL DEFAULT NULL,
  `ssid` int(11) NULL DEFAULT 0,
  `srid` int(11) NOT NULL DEFAULT 0,
  `strike` int(11) NOT NULL DEFAULT 1,
  `stts` int(11) NOT NULL DEFAULT 1,
  `sm_id` int(11) NULL DEFAULT NULL,
  `is_online` int(2) NULL DEFAULT 0,
  `is_pda` tinyint(1) NULL DEFAULT 1,
  PRIMARY KEY (`id`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 1018341 CHARACTER SET = latin1 COLLATE = latin1_swedish_ci ROW_FORMAT = Compact;
"""
CREATE_routes_TABLE_IF_NOT_EXISTS = """
CREATE TABLE IF NOT EXISTS `routes`  (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `mtime` timestamp(0) DEFAULT CURRENT_TIMESTAMP(0),
  `number` int(11) NOT NULL DEFAULT 1,
  `section` int(11) NOT NULL DEFAULT 498,
  `dsid` int(11) NOT NULL,
  `dpid` int(11) NOT NULL,
  `tsa_id` int(10) NULL DEFAULT NULL,
  `ssid` int(11) NULL DEFAULT 0,
  `srid` int(11) NOT NULL DEFAULT 0,
  `strike` int(11) NOT NULL DEFAULT 1,
  `stts` int(11) NOT NULL DEFAULT 1,
  `sm_id` int(11) NULL DEFAULT NULL,
  `is_online` int(2) NULL DEFAULT 0,
  `is_pda` tinyint(1) NULL DEFAULT 1,
  PRIMARY KEY (`id`) USING BTREE,
  INDEX `section_ind`(`section`) USING BTREE
) ENGINE = InnoDB CHARACTER SET = latin1 COLLATE = latin1_swedish_ci ROW_FORMAT = Compact;
"""

# -- ----------------------------
# -- Table structure for rtl_dimension
# -- ----------------------------
CREATE_rtl_dimension_TABLE_IF_NOT_EXISTS = """
CREATE TABLE IF NOT EXISTS `rtl_dimension`  (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `mtime` timestamp(0) NULL DEFAULT CURRENT_TIMESTAMP(0),
  `name` varchar(50) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL,
  `dpid` int(4) NULL DEFAULT NULL,
  `rtid` int(11) NULL DEFAULT 0,
  `channel` int(3) NULL DEFAULT 0,
  `subchannel` int(3) NULL DEFAULT 0,
  `TPG` int(3) NULL DEFAULT 0,
  `rtltype` int(3) NULL DEFAULT 0,
  `cname` int(4) NULL DEFAULT 0,
  `cluster_name` varchar(40) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL,
  `ctype` int(4) NULL DEFAULT 0,
  `gclass` int(4) NULL DEFAULT 0,
  `gtag` varchar(1) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL,
  `onature` int(1) NULL DEFAULT 0,
  `owner` varchar(50) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL,
  `contact` varchar(30) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL,
  `pinfo` varchar(20) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL,
  `fopp` int(11) NULL DEFAULT 0,
  `address` varchar(50) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL,
  `enrolltime` timestamp(0) NULL DEFAULT CURRENT_TIMESTAMP(0),
  `lastsaletime` datetime(0) NULL DEFAULT '1990-01-01 00:00:00',
  `stts` int(2) NULL DEFAULT NULL,
  `lastupdate` timestamp(0) NULL DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP(0),
  `smc` varchar(15) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL,
  `retailer_code` varchar(20) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL,
  `partner_code` varchar(20) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL,
  `platform_type` varchar(10) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL,
  `platform_code` varchar(20) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL,
  `dcc_scope` int(2) NULL DEFAULT 0,
  `reporting_sub` varchar(10) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL,
  PRIMARY KEY (`id`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 4632141 CHARACTER SET = utf8 COLLATE = utf8_general_ci ROW_FORMAT = Compact;
"""
# -- ----------------------------
# -- Table structure for sales_fact
# -- ----------------------------
CREATE_sales_fact_TABLE_IF_NOT_EXISTS = """
CREATE TABLE IF NOT EXISTS `sales_fact`  (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `mtime` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `rtslug` varchar(10) DEFAULT NULL,
  `dpid` int(4) NULL DEFAULT NULL,
  `route_id` int(11) NOT NULL,
  `rtlid` int(11) NOT NULL,
  `retailer_code` varchar(16) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL,
  `skid` int(4) NOT NULL,
  `volume` float(11,3) DEFAULT NULL,
  `value` float(7,2) DEFAULT NULL,
  `date` date NOT NULL DEFAULT '1990-01-01',
  `outlets` int(4) NOT NULL,
  `visited` int(4) NOT NULL,
  `channel` int(11) NOT NULL,
  `ioq` int(10) DEFAULT NULL,
  `sales_time` int(11) DEFAULT NULL,
  PRIMARY KEY (`id`) USING BTREE,
  INDEX `dp_ind`(`dpid`) USING BTREE,
  INDEX `route_ind`(`route_id`) USING BTREE,
  INDEX `sk_ind`(`skid`) USING BTREE,
  INDEX `date_ind`(`date`) USING BTREE,
  UNIQUE KEY unique_fields (rtlid,route_id,skid,date),
  CONSTRAINT `sales_fact_ibfk_1` FOREIGN KEY (`rtlid`) REFERENCES `rtl_dimension` (`id`) ON DELETE CASCADE ON UPDATE RESTRICT,
  CONSTRAINT `sales_fact_ibfk_2` FOREIGN KEY (`route_id`) REFERENCES `routes_dimension` (`id`) ON DELETE CASCADE ON UPDATE RESTRICT,
  CONSTRAINT `sales_fact_ibfk_3` FOREIGN KEY (`skid`) REFERENCES `sku_dimension` (`id`) ON DELETE CASCADE ON UPDATE RESTRICT,
  CONSTRAINT `sales_fact_ibfk_4` FOREIGN KEY (`channel`) REFERENCES `cats_dimension` (`id`) ON DELETE CASCADE ON UPDATE RESTRICT
) ENGINE = InnoDB AUTO_INCREMENT = 1 CHARACTER SET = latin1 COLLATE = latin1_swedish_ci ROW_FORMAT = Compact;
"""
# -- ----------------------------
# -- Table structure for staging_query_manager_table
# -- ----------------------------
CREATE_staging_query_manager_table_TABLE_IF_NOT_EXISTS = """
CREATE TABLE IF NOT EXISTS `staged_query_manager_table`  (
  `mtime` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `rtslug` varchar(10) DEFAULT NULL,
  `rtlid` int(11) NOT NULL,
  `skid` int(4) NOT NULL,
  `volume` float(11,3) DEFAULT NULL,
  `value` float(7,2) DEFAULT NULL,
  `date` date NOT NULL DEFAULT '1990-01-01',
  `outlets` int(4) NOT NULL,
  `visited` int(4) NOT NULL,
  `ioq` int(10) DEFAULT NULL,
  `sales_time` int(11) DEFAULT NULL,
  UNIQUE KEY `unique_fields` (`date`,`dpid`,`route_id`,`retailer_code`,`prid`,`skid`,`rtlid`) USING BTREE,
  PRIMARY KEY (`id`) USING BTREE,
  INDEX `rtl_ind`(`rtlid`) USING BTREE,
  INDEX `sk_ind`(`skid`) USING BTREE,
  CONSTRAINT `sales_fact_ibfk_1` FOREIGN KEY (`rtlid`) REFERENCES `rtl_dimension` (`id`) ON DELETE CASCADE ON UPDATE RESTRICT,
  CONSTRAINT `sales_fact_ibfk_2` FOREIGN KEY (`skid`) REFERENCES `sku_dimension` (`id`) ON DELETE CASCADE ON UPDATE RESTRICT
) ENGINE = InnoDB AUTO_INCREMENT = 1 CHARACTER SET = latin1 COLLATE = latin1_swedish_ci ROW_FORMAT = Compact;
"""
# -- ----------------------------
# -- Table structure for section_days_dimension
# -- ----------------------------
CREATE_section_days_dimension_TABLE_IF_NOT_EXISTS = """
CREATE TABLE IF NOT EXISTS `section_days_dimension`  (
  `id` int(11) NOT NULL,
  `mtime` timestamp(0) NOT NULL DEFAULT CURRENT_TIMESTAMP(0) ON UPDATE CURRENT_TIMESTAMP(0),
  `section` int(11) NOT NULL AUTO_INCREMENT,
  `slug` varchar(10) CHARACTER SET latin1 COLLATE latin1_swedish_ci NOT NULL,
  `saturday` int(11) NOT NULL,
  `sunday` int(11) NOT NULL,
  `monday` int(11) NOT NULL,
  `tuesday` int(11) NOT NULL,
  `wednesday` int(11) NOT NULL,
  `thursday` int(11) NOT NULL,
  `friday` int(11) NOT NULL,
  `working_days` int(11) NULL DEFAULT NULL,
  PRIMARY KEY (`section`) USING BTREE,
  INDEX `section`(`section`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 39 CHARACTER SET = latin1 COLLATE = latin1_swedish_ci ROW_FORMAT = Compact;
"""
# -- ----------------------------
# -- Table structure for sku_dimension
# -- ----------------------------
CREATE_sku_dimension_TABLE_IF_NOT_EXISTS = """
CREATE TABLE IF NOT EXISTS `sku_dimension`  (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `sku_type` enum('batb','we') CHARACTER SET latin1 COLLATE latin1_swedish_ci NULL DEFAULT 'batb',
  `mtime` timestamp(0) NOT NULL DEFAULT CURRENT_TIMESTAMP(0),
  `prid` int(11) NOT NULL,
  `sku` varchar(200) CHARACTER SET latin1 COLLATE latin1_swedish_ci NOT NULL,
  `material_code` int(11) NULL DEFAULT NULL,
  `ssku` varchar(100) CHARACTER SET latin1 COLLATE latin1_swedish_ci NOT NULL,
  `psize` int(11) NOT NULL,
  `ptype` int(11) NOT NULL,
  `dprice` float(7, 2) NOT NULL,
  `rprice` float(7, 2) NOT NULL,
  `dcc_price` float(7, 2) NOT NULL,
  `desk` varchar(1200) CHARACTER SET latin1 COLLATE latin1_swedish_ci NOT NULL,
  `stts` int(11) NOT NULL,
  `sort` float(11, 3) NULL DEFAULT NULL,
  `report_enable` int(1) NULL DEFAULT 1,
  `oneview_enable` int(1) NULL DEFAULT 1,
  `enable_sales` tinyint(1) NULL DEFAULT 1,
  PRIMARY KEY (`id`) USING BTREE,
  INDEX `pr_ind`(`prid`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 87 CHARACTER SET = latin1 COLLATE = latin1_swedish_ci ROW_FORMAT = Compact;
"""
# SET FOREIGN_KEY_CHECKS = 1;

CREATE_sales_by_sku_fact_TABLE_IF_NOT_EXISTS = """
CREATE TABLE IF NOT EXISTS `sales_by_sku_fact` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `dpid` int(11) DEFAULT NULL,
  `route_id` int(11) NOT NULL,
  `rtlid` int(11) NOT NULL,
  `retailer_code` varchar(16) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL,
  `skid` int(11) NOT NULL,
  `volume` float(11,3) DEFAULT NULL,
  `value` float(11,2) DEFAULT NULL,
  `date` date NOT NULL DEFAULT '1990-01-01',
  PRIMARY KEY (`id`) USING BTREE,
  UNIQUE KEY `unique_fields` (`rtlid`,`route_id`,`skid`,`date`),
  KEY `dp_ind` (`dpid`) USING BTREE,
  KEY `sk_ind` (`skid`) USING BTREE,
  KEY `date_ind` (`date`) USING BTREE,
  KEY `route_id` (`route_id`) USING BTREE,
  KEY `inx_report` (`dpid`,`date`) USING BTREE
) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=latin1 ROW_FORMAT=COMPACT
"""
CREATE_sales_by_brand_fact_TABLE_IF_NOT_EXISTS = """
CREATE TABLE IF NOT EXISTS `sales_by_brand_fact` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `dpid` int(11) DEFAULT NULL,
  `route_id` int(11) NOT NULL,
  `rtlid` int(11) NOT NULL,
  `retailer_code` varchar(16) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL,
  `prid` int(11) NOT NULL,
  `group` int(11) NOT NULL,
  `family` int(11) NOT NULL,
  `volume` float(11,3) DEFAULT NULL,
  `value` float(11,2) DEFAULT NULL,
  `date` date NOT NULL DEFAULT '1990-01-01',
  PRIMARY KEY (`id`) USING BTREE,
  UNIQUE KEY `unique_fields` (`rtlid`,`route_id`,`prid`,`date`),
  KEY `dp_ind` (`dpid`) USING BTREE,
  KEY `prid_ind` (`prid`) USING BTREE,
  KEY `date_ind` (`date`) USING BTREE,
  KEY `route_id` (`route_id`) USING BTREE,
  KEY `inx_report` (`dpid`,`date`) USING BTREE
) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=latin1 ROW_FORMAT=COMPACT
"""
# -- ----------------------------
# -- Table structure for hr_anomaly_dimension
# -- ----------------------------
CREATE_hr_anomaly_dimension_TABLE_IF_NOT_EXISTS = """
CREATE TABLE IF NOT EXISTS `hr_anomaly_dimension`  (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `sys_users_id` int(11) NOT NULL,
  `designations_id` int(11) NULL DEFAULT NULL,
  `route_number` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL,
  `anomaly_reason` text CHARACTER SET utf8 COLLATE utf8_general_ci NULL,
  `anomaly_details` longtext CHARACTER SET utf8 COLLATE utf8_general_ci NULL,
  `anomaly_status` int(11) NOT NULL DEFAULT 127,
  `anomaly_action` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL,
  `anomaly_date` date NULL DEFAULT NULL,
  `explain` text CHARACTER SET utf8 COLLATE utf8_general_ci NULL,
  `anomaly_level` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL,
  `created_by` int(10) NULL DEFAULT NULL,
  `created_at` timestamp(0) NULL DEFAULT NULL,
  `updated_by` int(10) NULL DEFAULT NULL,
  `updated_at` timestamp(0) NULL DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP(0),
  `status` enum('Active','Inactive') CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT 'Active',
  PRIMARY KEY (`id`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 229 CHARACTER SET = utf8 COLLATE = utf8_general_ci ROW_FORMAT = Dynamic;
"""
# SET FOREIGN_KEY_CHECKS = 1;
# -- ----------------------------
# -- Table structure for hr_emp_attendance_fact
# -- ----------------------------
CREATE_hr_emp_attendance_fact_TABLE_IF_NOT_EXISTS = """
CREATE TABLE IF NOT EXISTS `hr_emp_attendance_fact`  (
  `sys_users_id` int(10) UNSIGNED NOT NULL COMMENT 'sys_users->id',
  `user_code` varchar(20) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT 'employee id, user id, code',
  `att_designation` int(11) NULL DEFAULT NULL,
  `day_is` date NOT NULL,
  `is_salary_enabled` tinyint(1) NOT NULL DEFAULT 1 COMMENT '1 will consider for salary, 0 will consider to deduct salary',
  `bat_company_id` int(11) NULL DEFAULT NULL,
  `bat_dpid` int(11) NULL DEFAULT NULL,
  `route_number` text CHARACTER SET utf8 COLLATE utf8_general_ci NULL COMMENT 'comma separated route number',
  `hr_working_shifts_id` int(2) NOT NULL DEFAULT 1 COMMENT 'Active Shift name will store for that day',
  `shift_day_status` enum('W','H','R') CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT 'R',
  `shift_start_time` time(0) NOT NULL,
  `shift_end_time` time(0) NOT NULL,
  `hr_emp_sections_id` int(11) NULL DEFAULT NULL,
  `daily_status` enum('WP','W','P','L','H','HP','A','Lv','EO') CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL,
  `in_time` datetime(0) NULL DEFAULT NULL,
  `out_time` datetime(0) NULL DEFAULT NULL,
  `break_time` decimal(4, 2) NULL DEFAULT NULL,
  `total_work_time` decimal(4, 2) NULL DEFAULT NULL,
  `ot_hours` decimal(4, 2) NULL DEFAULT 0.00,
  `record_mode` enum('Device','ManualEntry') CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT 'Device',
  `approved_status` enum('locked','unlocked') CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT 'unlocked',
  `is_edited` int(2) NOT NULL DEFAULT 0 COMMENT 'increment for each time edit. 0 value define as not edited.',
  `file_name` int(11) NULL DEFAULT NULL,
  `alter_user_code` varchar(20) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL,
  `created_at` datetime(0) NULL DEFAULT NULL,
  `created_by` int(11) NULL DEFAULT 1,
  `updated_at` timestamp(0) NULL DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP(0),
  `updated_by` int(11) NULL DEFAULT NULL,
  `attn_status` enum('Active','Inactive') CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT 'Active',
  UNIQUE INDEX `uk_days_user_id`(`sys_users_id`, `day_is`, `bat_company_id`) USING BTREE,
  INDEX `day_is`(`day_is`) USING BTREE,
  INDEX `user_code`(`user_code`) USING BTREE
) ENGINE = InnoDB CHARACTER SET = utf8 COLLATE = utf8_general_ci ROW_FORMAT = Dynamic;
"""
# SET FOREIGN_KEY_CHECKS = 1;
# -- ----------------------------
# -- Table structure for hr_employee_record_logs
# -- ----------------------------
CREATE_hr_employee_record_logs_dimension_TABLE_IF_NOT_EXISTS = """
CREATE TABLE IF NOT EXISTS `hr_employee_record_logs_dimension`  (
  `hr_employee_record_logs_id` int(11) NOT NULL AUTO_INCREMENT,
  `sys_users_id` int(11) NULL DEFAULT NULL,
  `record_type` enum('promotion','transfer','salary_restructure') CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL,
  `designations_id` int(11) NULL DEFAULT NULL,
  `previous_designations_id` int(11) NULL DEFAULT NULL,
  `bat_company_id` int(11) NULL DEFAULT NULL,
  `bat_dpid` int(11) NULL DEFAULT NULL,
  `previous_dpid` int(11) NULL DEFAULT NULL,
  `applicable_date` date NULL DEFAULT NULL,
  `hr_emp_grades_id` int(11) NULL DEFAULT NULL,
  `previous_grades_id` int(11) NULL DEFAULT NULL,
  `basic_salary` decimal(10, 2) NULL DEFAULT NULL,
  `increment_type` enum('Yearly','Special') CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT 'Yearly',
  `increment_based_on` enum('basic','gross') CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT 'gross',
  `increment_amount` decimal(10, 2) NULL DEFAULT NULL,
  `gross_salary` decimal(10, 2) NULL DEFAULT NULL,
  `previous_gross` decimal(10, 2) NULL DEFAULT NULL,
  `created_by` int(10) NULL DEFAULT NULL,
  `created_at` datetime(0) NULL DEFAULT NULL,
  `updated_at` timestamp(0) NULL DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP(0),
  `updated_by` int(11) NULL DEFAULT NULL,
  `hr_transfer_status` int(11) NULL DEFAULT NULL,
  `hr_log_status` int(11) NULL DEFAULT NULL COMMENT 'a status id from sys_status_flow table',
  `status` enum('Active','Inactive') CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT 'Active',
  `is_manual` int(1) NULL DEFAULT 0,
  `approved_by` int(9) NULL DEFAULT NULL,
  PRIMARY KEY (`hr_employee_record_logs_id`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 2454 CHARACTER SET = utf8 COLLATE = utf8_general_ci ROW_FORMAT = Dynamic;
"""
# SET FOREIGN_KEY_CHECKS = 1;
CREATE_sys_users_fact_TABLE_IF_NOT_EXISTS = """
CREATE TABLE IF NOT EXISTS `sys_users_fact` (
  `id` int(10) UNSIGNED NOT NULL,
  `user_code` varchar(20) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL COMMENT 'Employee ID',
  `username` varchar(150) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL,
  `email` varchar(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NULL DEFAULT NULL,
  `is_employee` tinyint(1) NULL DEFAULT 0 COMMENT '0 for system user  ; 1 for employee (also can use system)',
  `name` varchar(100) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL,
  `name_bangla` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL,
  `email_verified_at` timestamp(0) NULL DEFAULT NULL,
  `mobile` varchar(20) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL,
  `date_of_birth` date NULL DEFAULT NULL,
  `blood_group` enum('A+','A-','B+','B-','O+','O-','AB+','AB-','') CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL,
  `gender` enum('Female','Male','') CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT 'Male',
  `religion` enum('Buddhist','Christian','Hindu','Islam','Others','') CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NULL DEFAULT 'Islam',
  `marital_status` enum('Married','Unmarried','Devorced','Widow','Single','') CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL,
  `nationality` enum('Bangladeshi') CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NULL DEFAULT 'Bangladeshi',
  `birth_certificate_no` varchar(30) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL,
  `nid` varchar(20) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL COMMENT 'NID or Birth certificate id will store in this same field',
  `tin` varchar(20) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL,
  `user_sign` varchar(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NULL DEFAULT NULL,
  `last_login` datetime(0) NULL DEFAULT NULL,
  `date_of_join` date NULL DEFAULT NULL,
  `date_of_confirmation` date NULL DEFAULT NULL,
  `is_reliever` tinyint(1) NULL DEFAULT 0,
  `reliever_to` int(10) NULL DEFAULT NULL,
  `reliever_start_datetime` datetime(0) NULL DEFAULT NULL,
  `reliever_end_datetime` datetime(0) NULL DEFAULT NULL,
  `bat_company_id` int(11) NULL DEFAULT NULL,
  `route_number` text CHARACTER SET utf8 COLLATE utf8_general_ci NULL COMMENT 'comma separated route number',
  `bat_dpid` int(11) NULL DEFAULT NULL COMMENT 'distributepoint->dpid',
  `privilege_houses` text CHARACTER SET utf8 COLLATE utf8_general_ci NULL COMMENT 'comma seperated distributor house ids',
  `privilege_points` longtext CHARACTER SET utf8 COLLATE utf8_general_ci NULL COMMENT 'comma seperated distributor point ids',
  `designations_id` int(10) NULL DEFAULT NULL,
  `departments_id` int(10) NULL DEFAULT NULL,
  `branchs_id` int(10) NULL DEFAULT 1,
  `hr_emp_grades_id` int(10) NULL DEFAULT NULL,
  `hr_emp_units_id` int(10) NULL DEFAULT NULL,
  `hr_emp_categorys_id` int(10) NULL DEFAULT 1,
  `hr_emp_sections_id` int(10) NULL DEFAULT NULL,
  `line_manager_id` int(10) NULL DEFAULT NULL,
  `hr_working_shifts_id` int(2) NULL DEFAULT 1 COMMENT 'Initially all employee assigned to the general shift',
  `start_time` time(0) NULL DEFAULT '09:00:00',
  `end_time` time(0) NULL DEFAULT '18:00:00',
  `basic_salary` decimal(10, 2) NULL DEFAULT 0.00,
  `other_conveyance` tinyint(2) NULL DEFAULT 0,
  `pf_amount_employee` decimal(10, 2) NULL DEFAULT NULL COMMENT 'total PF amount employee part',
  `pf_amount_company` decimal(10, 2) NULL DEFAULT 0.00 COMMENT 'total PF amount  company part',
  `gf_amount` decimal(10, 2) NULL DEFAULT 0.00 COMMENT 'total GF amount (employee part+ company part)',
  `insurance_amount` decimal(10, 2) NULL DEFAULT 0.00,
  `min_gross` decimal(10, 2) NULL DEFAULT 0.00,
  `applicable_date` date NULL DEFAULT NULL,
  `max_variable_salary` decimal(10, 2) NULL DEFAULT 0.00,
  `yearly_increment` decimal(4, 2) NULL DEFAULT 0.00 COMMENT 'A percentage value input for default increment.',
  `ot_applicable` enum('Hourly','Off Day','Both','Not Applicable') CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT 'Not Applicable',
  `pf_applicable` tinyint(1) NULL DEFAULT 0 COMMENT 'providend fund applicable or not',
  `gf_applicable` tinyint(1) NULL DEFAULT 0 COMMENT '1 = Gratuity fund applicable',
  `insurance_applicable` tinyint(1) NULL DEFAULT 0 COMMENT '1 = insurance will applicable for this grad employee',
  `late_deduction_applied` tinyint(1) NULL DEFAULT 0 COMMENT '1 = late deduction will applicable for this employee',
  `default_salary_applied` tinyint(1) NULL DEFAULT 1 COMMENT '0 if not default salary structure followed',
  `salary_disburse_type` enum('Bank','MFS') CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT 'Bank',
  `mfs_account_name_old` varchar(100) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL COMMENT 'mobile banking account holder name',
  `salary_account_no` varchar(30) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL,
  `reference_user_id` int(10) NULL DEFAULT NULL COMMENT 'existing user id who refer this employee to join. if any',
  `geo_location_7_id` int(9) NOT NULL DEFAULT 1 COMMENT 'Geo Location Relationship',
  `created_by` int(10) NULL DEFAULT NULL,
  `created_at` datetime(0) NULL DEFAULT NULL,
  `updated_at` timestamp(0) NULL DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP(0),
  `updated_by` int(11) NULL DEFAULT 0,
  `is_roaster` tinyint(4) NULL DEFAULT 0,
  `status` enum('Active','Inactive','Resignation','Termination','Absconding','Separated','Retirement','Probation') CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT 'Active',
  `working_type` enum('Full Time','Part Time','Contractual','On Call') CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT 'Full Time',
  `separation_date` date NULL DEFAULT NULL,
  `hr_separation_causes` text CHARACTER SET utf8 COLLATE utf8_general_ci NULL COMMENT 'Multiple Cause may selected',
  `leave_policy_apply` tinyint(1) NULL DEFAULT 0,
  `is_transfer` tinyint(1) NULL DEFAULT NULL,
  `identity_type` enum('NID','Birth Certificate','Passport','Driving License') CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT 'NID',
  `identity_number` varchar(155) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL,
  `sub_designation` int(10) NULL DEFAULT NULL,
  `mfs_account_number` varchar(55) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL,
  `mfs_account_name` enum('bKash','Nagad') CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL,
  PRIMARY KEY (`id`) USING BTREE,
  UNIQUE INDEX `uk_sys_users_user_code`(`user_code`) USING BTREE,
  UNIQUE INDEX `users_name_unique`(`username`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 23409 CHARACTER SET = utf8 COLLATE = utf8_general_ci COMMENT = 'All user could be an employee but all employee not be an user. So all information in this table consider as employee information.' ROW_FORMAT = Dynamic;
"""
# SET FOREIGN_KEY_CHECKS = 1;
CREATE_designations_dimension_TABLE_IF_NOT_EXISTS = """
CREATE TABLE IF NOT EXISTS `designations_dimension`  (
  `designations_id` int(11) NOT NULL AUTO_INCREMENT,
  `designations_name` varchar(100) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL,
  `short_name` varchar(20) CHARACTER SET latin1 COLLATE latin1_swedish_ci NULL DEFAULT NULL,
  `order_position` int(9) NULL DEFAULT NULL,
  `hr_emp_grade_id` int(11) NULL DEFAULT NULL,
  `minimum_gross` decimal(10, 0) NULL DEFAULT NULL,
  `minimum_total` decimal(10, 0) NULL DEFAULT NULL,
  `created_at` datetime(0) NULL DEFAULT NULL,
  `created_by` int(11) NOT NULL,
  `updated_at` timestamp(0) NULL DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP(0),
  `updated_by` int(11) NULL DEFAULT NULL,
  `status` enum('Inactive','Active') CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL,
  PRIMARY KEY (`designations_id`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 584 CHARACTER SET = latin1 COLLATE = latin1_swedish_ci ROW_FORMAT = Dynamic;
"""

CREATE_sub_designations_dimension_TABLE_IF_NOT_EXISTS = """
CREATE TABLE IF NOT EXISTS `sub_designations_dimension`  (
  `sub_designations_id` int(11) NOT NULL AUTO_INCREMENT,
  `name` varchar(100) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL,
  `short_name` varchar(20) CHARACTER SET latin1 COLLATE latin1_swedish_ci NULL DEFAULT NULL,
  `order_position` int(9) NULL DEFAULT NULL,
  `created_at` datetime(0) NULL DEFAULT NULL,
  `created_by` int(11) NOT NULL,
  `updated_at` timestamp(0) NULL DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP(0),
  `updated_by` int(11) NULL DEFAULT NULL,
  `status` enum('Inactive','Active') CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL,
  PRIMARY KEY (`sub_designations_id`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 584 CHARACTER SET = latin1 COLLATE = latin1_swedish_ci ROW_FORMAT = Dynamic;
"""
CREATE_bat_locations_dimension_TABLE_IF_NOT_EXISTS = """
CREATE TABLE IF NOT EXISTS `bat_locations_dimension`  (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `mtime` timestamp(0) NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP(0),
  `parent` int(11) NOT NULL DEFAULT 0,
  `ltype` int(11) NOT NULL DEFAULT 0,
  `slug` varchar(200) CHARACTER SET latin1 COLLATE latin1_swedish_ci NOT NULL,
  `stts` int(11) NOT NULL DEFAULT 0,
  PRIMARY KEY (`id`) USING BTREE,
  INDEX `stts`(`stts`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 315 CHARACTER SET = latin1 COLLATE = latin1_swedish_ci ROW_FORMAT = Dynamic;
"""

CREATE_geo_distance_by_outlet_fact_TABLE_IF_NOT_EXISTS = """
CREATE TABLE IF NOT EXISTS `geo_distance_by_outlet_fact` (
  `retailer_id` int(11) DEFAULT NULL,
  `retailer_code` varchar(13) DEFAULT NULL,
  `distance` float(11,2) DEFAULT NULL,
  `rtid` int(11) DEFAULT NULL,
  `dpid` int(11) DEFAULT NULL,
  `date` date DEFAULT NULL,
  `sales_time` datetime DEFAULT NULL,
  `sr_id` int(11) DEFAULT NULL,
  `allowable_distance` int(11) DEFAULT '100',
  `visited` int(5) DEFAULT '1',
  `hit` int(5) DEFAULT '0',
  `location_change` int(5) DEFAULT '0',
  `valid` int(5) DEFAULT '0',
  `invalid` int(2) DEFAULT '0',
  `internet_speed` varchar(10) DEFAULT '0',
  `created` datetime DEFAULT CURRENT_TIMESTAMP,
  KEY `uni` (`retailer_id`,`date`,`rtid`),
  KEY `date` (`date`) USING BTREE,
  KEY `dpid` (`dpid`) USING BTREE
) ENGINE=InnoDB DEFAULT CHARSET=latin1 COMMENT='geo_status: 1= Hit,  2 = Location Change, 3 = Valid, 4 = Invalid'
"""
CREATE_geo_distance_by_section_fact_TABLE_IF_NOT_EXISTS = """
CREATE TABLE IF NOT EXISTS `geo_distance_by_section_fact` (
  `rtid` int(11) DEFAULT NULL,
  `dpid` int(11) DEFAULT NULL,
  `date` date DEFAULT NULL,
  `sr_id` int(11) DEFAULT NULL,
  `allowable_distance` int(11) DEFAULT '100',
  `visited` int(10) DEFAULT '1',
  `hit` int(10) DEFAULT '0',
  `location_change` int(10) DEFAULT '0',
  `valid` int(10) DEFAULT '0',
  `invalid` int(10) DEFAULT '0',
  `internet_speed` varchar(10) DEFAULT '0',
  `created` datetime DEFAULT CURRENT_TIMESTAMP,
  KEY `uni` (`date`,`rtid`),
  KEY `date` (`date`) USING BTREE,
  KEY `dpid` (`dpid`) USING BTREE
) ENGINE=InnoDB DEFAULT CHARSET=latin1
"""
