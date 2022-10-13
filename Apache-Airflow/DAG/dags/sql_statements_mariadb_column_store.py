CREATE_cats_dimension_TABLE_IF_NOT_EXISTS = """
CREATE TABLE IF NOT EXISTS `cats_dimension` (
  `id` int(11) NOT NULL,
  `mtime` timestamp NOT NULL DEFAULT current_timestamp() ON UPDATE current_timestamp(),
  `parent` int(11) NOT NULL DEFAULT 0,
  `inherit` int(11) NOT NULL DEFAULT 0,
  `slug` varchar(200) COLLATE utf8_unicode_ci NOT NULL,
  `display_label` varchar(50) COLLATE utf8_unicode_ci DEFAULT NULL,
  `partner_tpg` int(6) DEFAULT NULL,
  `flag` int(11) NOT NULL DEFAULT 0,
  `stts` int(11) NOT NULL DEFAULT 0,
  `sorting_order` int(3) DEFAULT NULL
) ENGINE=Columnstore DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci
"""
CREATE_company_dimension_TABLE_IF_NOT_EXISTS = """
CREATE TABLE IF NOT EXISTS `company_dimension` (
  `id` int(11) NOT NULL,
  `mtime` timestamp NOT NULL DEFAULT current_timestamp() ON UPDATE current_timestamp(),
  `name` varchar(200) COLLATE utf8_unicode_ci NOT NULL DEFAULT 'NAME',
  `owner` varchar(200) COLLATE utf8_unicode_ci NOT NULL DEFAULT 'OWNER',
  `uname` varchar(50) COLLATE utf8_unicode_ci NOT NULL DEFAULT 'UNAME',
  `pass` varchar(32) COLLATE utf8_unicode_ci NOT NULL DEFAULT 'PASS',
  `email` varchar(200) COLLATE utf8_unicode_ci NOT NULL,
  `address` varchar(255) COLLATE utf8_unicode_ci NOT NULL DEFAULT 'ADDRESS',
  `contact` varchar(200) COLLATE utf8_unicode_ci NOT NULL DEFAULT 'CONTACT',
  `desk` varchar(2000) COLLATE utf8_unicode_ci NOT NULL DEFAULT 'DESK',
  `short_name` varchar(100) COLLATE utf8_unicode_ci DEFAULT NULL,
  `stock_start_date` date DEFAULT '2015-10-27',
  `stts` int(11) NOT NULL,
  `region` int(20) NOT NULL,
  `area` int(20) NOT NULL,
  `updated` timestamp NOT NULL DEFAULT '0000-00-00 00:00:00',
  `sales_type` varchar(200) COLLATE utf8_unicode_ci DEFAULT 'BAT',
  `no_of_points` int(10) DEFAULT NULL
) ENGINE=Columnstore DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci
"""
CREATE_daily_sales_msr_fact_TABLE_IF_NOT_EXISTS = """
CREATE TABLE IF NOT EXISTS `daily_sales_msr_fact` (
  `id` int(11) NOT NULL,
  `mtime` timestamp NOT NULL DEFAULT current_timestamp() ON UPDATE current_timestamp(),
  `dpid` int(11) NOT NULL,
  `route_id` int(11) NOT NULL,
  `skid` int(11) NOT NULL,
  `prid` int(11) NOT NULL,
  `group` int(11) NOT NULL,
  `family` int(11) NOT NULL,
  `date` date NOT NULL DEFAULT '1990-01-01',
  `sale` double NOT NULL,
  `dprice` double NOT NULL,
  `rprice` double NOT NULL,
  `dcc_price` double NOT NULL,
  `issue` double NOT NULL,
  `return` double DEFAULT NULL,
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
  `snb_cnc` double DEFAULT NULL,
  `pay_n_go` double DEFAULT NULL,
  `shop_n_browse` double DEFAULT NULL,
  `entertainment` double DEFAULT NULL,
  `outlets` int(11) NOT NULL,
  `apps_version` varchar(20) COLLATE utf8_unicode_ci DEFAULT NULL,
  `updated` timestamp NULL DEFAULT NULL,
  `visited` int(11) NOT NULL
) ENGINE=Columnstore DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci
"""
CREATE_distributorspoint_dimension_TABLE_IF_NOT_EXISTS = """
CREATE TABLE IF NOT EXISTS `distributorspoint_dimension` (
  `id` int(11) NOT NULL,
  `mtime` timestamp NOT NULL DEFAULT current_timestamp() ON UPDATE current_timestamp(),
  `dsid` int(11) NOT NULL COMMENT 'dsid = company id',
  `region` int(11) NOT NULL,
  `area` int(11) NOT NULL,
  `territory` int(11) NOT NULL,
  `name` varchar(200) COLLATE utf8_unicode_ci NOT NULL DEFAULT '',
  `email` varchar(200) COLLATE utf8_unicode_ci NOT NULL DEFAULT '',
  `pass` varchar(32) COLLATE utf8_unicode_ci NOT NULL DEFAULT '',
  `contact` varchar(200) COLLATE utf8_unicode_ci NOT NULL DEFAULT '',
  `address` varchar(200) COLLATE utf8_unicode_ci NOT NULL DEFAULT '',
  `skus` varchar(200) COLLATE utf8_unicode_ci NOT NULL DEFAULT '',
  `de_date` date DEFAULT NULL,
  `stock_start_date` date DEFAULT '2015-10-27',
  `pda` int(11) NOT NULL DEFAULT 0,
  `stts` int(11) NOT NULL,
  `updated` timestamp NOT NULL DEFAULT '0000-00-00 00:00:00',
  `sales_type` varchar(200) COLLATE utf8_unicode_ci DEFAULT 'BAT',
  `we_point` tinyint(4) DEFAULT 0,
  `we_skus` varchar(200) COLLATE utf8_unicode_ci DEFAULT NULL
) ENGINE=Columnstore DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci
"""
CREATE_locations_dimension_TABLE_IF_NOT_EXISTS = """
CREATE TABLE IF NOT EXISTS `locations_dimension` (
  `id` int(11) NOT NULL,
  `mtime` timestamp NOT NULL DEFAULT current_timestamp() ON UPDATE current_timestamp(),
  `parent` int(11) NOT NULL DEFAULT 0,
  `ltype` int(11) NOT NULL DEFAULT 0,
  `slug` varchar(200) COLLATE utf8_unicode_ci NOT NULL,
  `stts` int(11) NOT NULL DEFAULT 0
) ENGINE=Columnstore DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci
"""
CREATE_products_dimension_TABLE_IF_NOT_EXISTS = """
CREATE TABLE IF NOT EXISTS `products_dimension` (
  `id` int(11) NOT NULL,
  `mtime` timestamp NOT NULL DEFAULT current_timestamp() ON UPDATE current_timestamp(),
  `name` varchar(200) COLLATE utf8_unicode_ci NOT NULL,
  `family` int(11) NOT NULL,
  `group` int(11) NOT NULL,
  `color` varchar(32) COLLATE utf8_unicode_ci NOT NULL,
  `desk` varchar(1200) COLLATE utf8_unicode_ci NOT NULL,
  `flags` int(11) NOT NULL,
  `targetvalue` float NOT NULL,
  `oosr` float NOT NULL,
  `ioq` int(11) NOT NULL,
  `stts` int(11) NOT NULL,
  `sort` float(11,3) DEFAULT NULL,
  `report_enable` float(11,3) DEFAULT 1.000,
  `is_new` tinyint(4) DEFAULT 0
) ENGINE=Columnstore DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci
"""
CREATE_routes_dimension_TABLE_IF_NOT_EXISTS = """
CREATE TABLE IF NOT EXISTS `routes_dimension` (
  `id` int(11) NOT NULL,
  `mtime` timestamp NOT NULL DEFAULT current_timestamp() ON UPDATE current_timestamp(),
  `number` int(11) NOT NULL DEFAULT 1,
  `section` int(11) NOT NULL DEFAULT 498,
  `dsid` int(11) NOT NULL,
  `dpid` int(11) NOT NULL,
  `tsa_id` int(10) DEFAULT NULL,
  `ssid` int(11) NOT NULL DEFAULT 0,
  `srid` int(11) NOT NULL DEFAULT 0,
  `strike` int(11) NOT NULL DEFAULT 1,
  `stts` int(11) NOT NULL DEFAULT 1,
  `sm_id` int(11) DEFAULT NULL,
  `is_online` int(2) DEFAULT 0,
  `is_pda` tinyint(1) DEFAULT 1
) ENGINE=Columnstore DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci
"""
CREATE_rtl_dimension_TABLE_IF_NOT_EXISTS = """
CREATE TABLE IF NOT EXISTS `rtl_dimension` (
  `id` int(11) NOT NULL,
  `mtime` timestamp NOT NULL DEFAULT current_timestamp() ON UPDATE current_timestamp(),
  `name` varchar(50) COLLATE utf8_unicode_ci DEFAULT NULL,
  `dpid` int(4) DEFAULT NULL,
  `rtid` int(11) DEFAULT 0,
  `channel` int(3) DEFAULT 0,
  `subchannel` int(3) DEFAULT 0,
  `TPG` int(3) DEFAULT 0,
  `rtltype` int(3) DEFAULT 0,
  `cname` int(4) DEFAULT 0,
  `cluster_name` varchar(40) COLLATE utf8_unicode_ci DEFAULT NULL,
  `ctype` int(4) DEFAULT 0,
  `gclass` int(4) DEFAULT 0,
  `gtag` varchar(1) COLLATE utf8_unicode_ci DEFAULT NULL,
  `onature` int(1) DEFAULT 0,
  `owner` varchar(50) COLLATE utf8_unicode_ci DEFAULT NULL,
  `contact` varchar(30) COLLATE utf8_unicode_ci DEFAULT NULL,
  `pinfo` varchar(20) COLLATE utf8_unicode_ci DEFAULT NULL,
  `fopp` int(11) DEFAULT 0,
  `address` varchar(50) COLLATE utf8_unicode_ci DEFAULT NULL,
  `enrolltime` timestamp NOT NULL DEFAULT '0000-00-00 00:00:00',
  `lastsaletime` datetime DEFAULT '1990-01-01 00:00:00',
  `stts` int(2) DEFAULT NULL,
  `lastupdate` timestamp NULL DEFAULT NULL,
  `smc` varchar(15) COLLATE utf8_unicode_ci DEFAULT NULL,
  `retailer_code` varchar(20) COLLATE utf8_unicode_ci DEFAULT NULL,
  `partner_code` varchar(20) COLLATE utf8_unicode_ci DEFAULT NULL,
  `platform_type` varchar(10) COLLATE utf8_unicode_ci DEFAULT NULL,
  `platform_code` varchar(20) COLLATE utf8_unicode_ci DEFAULT NULL,
  `dcc_scope` int(2) DEFAULT 0,
  `reporting_sub` varchar(10) COLLATE utf8_unicode_ci DEFAULT NULL
) ENGINE=Columnstore DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci
"""
CREATE_sales_by_brand_fact_TABLE_IF_NOT_EXISTS = """
CREATE TABLE IF NOT EXISTS `sales_by_brand_fact` (
  `id` int(11) NOT NULL,
  `dpid` int(11) DEFAULT NULL,
  `route_id` int(11) NOT NULL,
  `rtlid` int(11) NOT NULL,
  `retailer_code` varchar(16) COLLATE utf8_unicode_ci DEFAULT NULL,
  `prid` int(11) NOT NULL,
  `group` int(11) NOT NULL,
  `family` int(11) NOT NULL,
  `volume` float(11,3) DEFAULT NULL,
  `value` float(11,2) DEFAULT NULL,
  `date` date NOT NULL DEFAULT '1990-01-01'
) ENGINE=Columnstore DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci
"""
CREATE_sales_by_sku_fact_TABLE_IF_NOT_EXISTS = """
CREATE TABLE IF NOT EXISTS `sales_by_sku_fact` (
  `id` int(11) NOT NULL,
  `dpid` int(11) DEFAULT NULL,
  `route_id` int(11) NOT NULL,
  `rtlid` int(11) NOT NULL,
  `retailer_code` varchar(16) COLLATE utf8_unicode_ci DEFAULT NULL,
  `skid` int(11) NOT NULL,
  `volume` float(11,3) DEFAULT NULL,
  `value` float(11,2) DEFAULT NULL,
  `date` date NOT NULL DEFAULT '1990-01-01'
) ENGINE=Columnstore DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci
"""
CREATE_section_days_dimension_TABLE_IF_NOT_EXISTS = """
CREATE TABLE IF NOT EXISTS `section_days_dimension` (
  `id` int(11) NOT NULL,
  `mtime` timestamp NOT NULL DEFAULT current_timestamp() ON UPDATE current_timestamp(),
  `section` int(11) NOT NULL,
  `slug` varchar(10) COLLATE utf8_unicode_ci NOT NULL,
  `saturday` int(11) NOT NULL,
  `sunday` int(11) NOT NULL,
  `monday` int(11) NOT NULL,
  `tuesday` int(11) NOT NULL,
  `wednesday` int(11) NOT NULL,
  `thursday` int(11) NOT NULL,
  `friday` int(11) NOT NULL,
  `working_days` int(11) DEFAULT NULL
) ENGINE=Columnstore DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci
"""
CREATE_sku_dimension_TABLE_IF_NOT_EXISTS = """
CREATE TABLE IF NOT EXISTS `sku_dimension` (
  `id` int(11) NOT NULL,
  `sku_type` varchar(200) COLLATE utf8_unicode_ci DEFAULT 'batb',
  `mtime` timestamp NOT NULL DEFAULT current_timestamp() ON UPDATE current_timestamp(),
  `prid` int(11) NOT NULL,
  `sku` varchar(200) COLLATE utf8_unicode_ci NOT NULL,
  `material_code` int(11) DEFAULT NULL,
  `ssku` varchar(100) COLLATE utf8_unicode_ci NOT NULL,
  `psize` int(11) NOT NULL,
  `ptype` int(11) NOT NULL,
  `dprice` float(7,2) NOT NULL,
  `rprice` float(7,2) NOT NULL,
  `dcc_price` float(7,2) NOT NULL,
  `desk` varchar(1200) COLLATE utf8_unicode_ci NOT NULL,
  `stts` int(11) NOT NULL,
  `sort` float(11,3) DEFAULT NULL,
  `report_enable` int(1) DEFAULT 1,
  `oneview_enable` int(1) DEFAULT 1,
  `enable_sales` tinyint(1) DEFAULT 1
) ENGINE=Columnstore DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci
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
  `created` datetime
) ENGINE=Columnstore DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;
"""
CREATE_geo_distance_by_outlet_fact_TABLE_IF_NOT_EXISTS = """
CREATE TABLE IF NOT EXISTS `geo_distance_by_outlet_fact`  (
  `retailer_id` int(11) NULL DEFAULT NULL,
  `retailer_code` varchar(13) CHARACTER SET utf8 COLLATE utf8_unicode_ci NULL DEFAULT NULL,
  `distance` float(11, 2) NULL DEFAULT NULL,
  `rtid` int(11) NULL DEFAULT NULL,
  `dpid` int(11) NULL DEFAULT NULL,
  `date` date NULL DEFAULT NULL,
  `sales_time` datetime(0) NULL DEFAULT NULL,
  `sr_id` int(11) NULL DEFAULT NULL,
  `allowable_distance` int(11) NULL DEFAULT 100,
  `visited` int(5) NULL DEFAULT 1,
  `hit` int(5) NULL DEFAULT 0,
  `location_change` int(5) NULL DEFAULT 0,
  `valid` int(5) NULL DEFAULT 0,
  `invalid` int(2) NULL DEFAULT 0,
  `internet_speed` varchar(10) CHARACTER SET utf8 COLLATE utf8_unicode_ci NULL DEFAULT '0',
  `created` datetime(0) NULL DEFAULT NULL
) ENGINE = Columnstore CHARACTER SET = utf8 COLLATE = utf8_unicode_ci ROW_FORMAT = Dynamic;
"""
CREATE_hr_anomaly_dimension_TABLE_IF_NOT_EXISTS = """
CREATE TABLE IF NOT EXISTS `hr_anomaly_dimension`  (
  `id` int(11) NOT NULL,
  `sys_users_id` int(11) NOT NULL,
  `designations_id` int(11) NULL DEFAULT NULL,
  `route_number` varchar(255) CHARACTER SET utf8 COLLATE utf8_unicode_ci NULL DEFAULT NULL,
  `anomaly_reason` varchar(255) CHARACTER SET utf8 COLLATE utf8_unicode_ci NULL DEFAULT NULL,
  `anomaly_details` varchar(255) CHARACTER SET utf8 COLLATE utf8_unicode_ci NULL DEFAULT NULL,
  `anomaly_status` int(11) NOT NULL DEFAULT 127,
  `anomaly_action` varchar(255) CHARACTER SET utf8 COLLATE utf8_unicode_ci NULL DEFAULT NULL,
  `anomaly_date` timestamp(0) NOT NULL DEFAULT current_timestamp ON UPDATE CURRENT_TIMESTAMP,
  `explain` varchar(255) CHARACTER SET utf8 COLLATE utf8_unicode_ci NULL DEFAULT NULL,
  `anomaly_level` varchar(255) CHARACTER SET utf8 COLLATE utf8_unicode_ci NULL DEFAULT NULL,
  `created_by` int(10) NULL DEFAULT NULL,
  `created_at` timestamp(0) NOT NULL DEFAULT '0000-00-00 00:00:00',
  `updated_by` int(10) NULL DEFAULT NULL,
  `updated_at` timestamp(0) NOT NULL DEFAULT '0000-00-00 00:00:00',
  `status` varchar(255) CHARACTER SET utf8 COLLATE utf8_unicode_ci NOT NULL
) ENGINE = Columnstore CHARACTER SET = utf8 COLLATE = utf8_unicode_ci ROW_FORMAT = Dynamic;
"""
CREATE_hr_emp_attendance_fact_TABLE_IF_NOT_EXISTS = """
CREATE TABLE IF NOT EXISTS `hr_emp_attendance_fact`  (
  `sys_users_id` int(10) NOT NULL,
  `user_code` varchar(20) CHARACTER SET utf8 COLLATE utf8_unicode_ci NULL DEFAULT NULL,
  `att_designation` int(11) NULL DEFAULT NULL,
  `day_is` date NOT NULL,
  `is_salary_enabled` tinyint(1) NOT NULL DEFAULT 1,
  `bat_company_id` int(11) NULL DEFAULT NULL,
  `bat_dpid` int(11) NULL DEFAULT NULL,
  `route_number` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL,
  `hr_working_shifts_id` int(2) NOT NULL DEFAULT 1,
  `shift_day_status` varchar(255) CHARACTER SET utf8 COLLATE utf8_unicode_ci NULL DEFAULT NULL,
  `shift_start_time` time(0) NOT NULL,
  `shift_end_time` time(0) NOT NULL,
  `hr_emp_sections_id` int(11) NULL DEFAULT NULL,
  `daily_status` varchar(255) CHARACTER SET utf8 COLLATE utf8_unicode_ci NULL DEFAULT NULL,
  `in_time` timestamp(0) NULL DEFAULT NULL,
  `out_time` timestamp(0) NULL DEFAULT NULL,
  `break_time` decimal(4, 2) NULL DEFAULT NULL,
  `total_work_time` decimal(4, 2) NULL DEFAULT NULL,
  `ot_hours` decimal(4, 2) NULL DEFAULT 0,
  `record_mode` varchar(255) CHARACTER SET utf8 COLLATE utf8_unicode_ci NULL DEFAULT NULL,
  `approved_status` varchar(255) CHARACTER SET utf8 COLLATE utf8_unicode_ci NULL DEFAULT NULL,
  `is_edited` int(2) NOT NULL DEFAULT 0,
  `file_name` int(11) NULL DEFAULT NULL,
  `alter_user_code` varchar(20) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL,
  `created_at` timestamp(0) NULL DEFAULT NULL,
  `created_by` int(11) NULL DEFAULT 1,
  `updated_at` timestamp(0) NULL DEFAULT NULL,
  `updated_by` int(11) NULL DEFAULT NULL,
  `attn_status` varchar(255) CHARACTER SET utf8 COLLATE utf8_unicode_ci NULL DEFAULT NULL
) ENGINE = Columnstore CHARACTER SET = utf8 COLLATE = utf8_unicode_ci ROW_FORMAT = Dynamic;
"""
CREATE_hr_employee_record_logs_dimension_TABLE_IF_NOT_EXISTS = """
CREATE TABLE IF NOT EXISTS `hr_employee_record_logs_dimension`  (
  `hr_employee_record_logs_id` int(11) NOT NULL,
  `sys_users_id` int(11) NULL DEFAULT NULL,
  `record_type` varchar(255) CHARACTER SET utf8 COLLATE utf8_unicode_ci NULL DEFAULT NULL,
  `designations_id` int(11) NULL DEFAULT NULL,
  `previous_designations_id` int(11) NULL DEFAULT NULL,
  `bat_company_id` int(11) NULL DEFAULT NULL,
  `bat_dpid` int(11) NULL DEFAULT NULL,
  `previous_dpid` int(11) NULL DEFAULT NULL,
  `applicable_date` date NULL DEFAULT NULL,
  `hr_emp_grades_id` int(11) NULL DEFAULT NULL,
  `previous_grades_id` int(11) NULL DEFAULT NULL,
  `basic_salary` decimal(10, 2) NULL DEFAULT NULL,
  `increment_type` varchar(255) CHARACTER SET utf8 COLLATE utf8_unicode_ci NULL DEFAULT NULL,
  `increment_based_on` varchar(255) CHARACTER SET utf8 COLLATE utf8_unicode_ci NULL DEFAULT NULL,
  `increment_amount` decimal(10, 2) NULL DEFAULT NULL,
  `gross_salary` decimal(10, 2) NULL DEFAULT NULL,
  `previous_gross` decimal(10, 2) NULL DEFAULT NULL,
  `created_by` int(10) NULL DEFAULT NULL,
  `created_at` datetime(0) NULL DEFAULT NULL,
  `updated_at` timestamp(0) NULL DEFAULT NULL,
  `updated_by` int(11) NULL DEFAULT NULL,
  `hr_transfer_status` int(11) NULL DEFAULT NULL,
  `hr_log_status` int(11) NULL DEFAULT NULL,
  `status` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT 'Active',
  `is_manual` int(1) NULL DEFAULT 0,
  `approved_by` int(9) NULL DEFAULT NULL
) ENGINE = Columnstore CHARACTER SET = utf8 COLLATE = utf8_unicode_ci ROW_FORMAT = Dynamic;
"""
CREATE_designations_dimension_TABLE_IF_NOT_EXISTS = """
CREATE TABLE IF NOT EXISTS `designations_dimension`  (
  `designations_id` int(11) NOT NULL,
  `designations_name` varchar(100) CHARACTER SET utf8 COLLATE utf8_unicode_ci NULL DEFAULT NULL,
  `short_name` varchar(20) CHARACTER SET utf8 COLLATE utf8_unicode_ci NULL DEFAULT NULL,
  `order_position` int(9) NULL DEFAULT NULL,
  `hr_emp_grade_id` int(11) NULL DEFAULT NULL,
  `minimum_gross` decimal(10, 0) NULL DEFAULT NULL,
  `minimum_total` decimal(10, 0) NULL DEFAULT NULL,
  `created_at` datetime(0) NULL DEFAULT NULL,
  `created_by` int(11) NOT NULL,
  `updated_at` timestamp(0) NULL DEFAULT NULL,
  `updated_by` int(11) NULL DEFAULT NULL,
  `status` varchar(255) CHARACTER SET utf8 COLLATE utf8_unicode_ci NULL DEFAULT NULL
) ENGINE = Columnstore CHARACTER SET = utf8 COLLATE = utf8_unicode_ci ROW_FORMAT = Dynamic;
"""
CREATE_sub_designations_dimension_TABLE_IF_NOT_EXISTS = """
CREATE TABLE IF NOT EXISTS `sub_designations_dimension`  (
  `sub_designations_id` int(11) NOT NULL,
  `name` varchar(100),
  `short_name` varchar(20),
  `order_position` int(9) NULL DEFAULT NULL,
  `created_at` datetime(0) NULL DEFAULT NULL,
  `created_by` int(11) NOT NULL,
  `updated_at` timestamp(0) NULL DEFAULT NULL,
  `updated_by` int(11) NULL DEFAULT NULL,
  `status` VARCHAR(255)
) ENGINE=Columnstore DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;
"""

CREATE_sys_users_fact_TABLE_IF_NOT_EXISTS = """
CREATE TABLE IF NOT EXISTS `sys_users_fact`  (
  `id` int(10) NOT NULL,
  `user_code` varchar(20) CHARACTER SET utf8 COLLATE utf8_unicode_ci NULL DEFAULT NULL,
  `username` varchar(150) CHARACTER SET utf8 COLLATE utf8_unicode_ci NULL DEFAULT NULL,
  `email` varchar(100) CHARACTER SET utf8 COLLATE utf8_unicode_ci NULL DEFAULT NULL,
  `is_employee` tinyint(1) NULL DEFAULT 0,
  `name` varchar(100) CHARACTER SET utf8 COLLATE utf8_unicode_ci NULL DEFAULT NULL,
  `name_bangla` varchar(255) CHARACTER SET utf8 COLLATE utf8_unicode_ci NULL DEFAULT NULL,
  `email_verified_at` timestamp(0) NULL DEFAULT NULL,
  `mobile` varchar(20) CHARACTER SET utf8 COLLATE utf8_unicode_ci NULL DEFAULT NULL,
  `date_of_birth` date NULL DEFAULT NULL,
  `blood_group` varchar(255) CHARACTER SET utf8 COLLATE utf8_unicode_ci NULL DEFAULT NULL,
  `gender` varchar(255) CHARACTER SET utf8 COLLATE utf8_unicode_ci NULL DEFAULT NULL,
  `religion` varchar(255) CHARACTER SET utf8 COLLATE utf8_unicode_ci NULL DEFAULT NULL,
  `marital_status` varchar(255) CHARACTER SET utf8 COLLATE utf8_unicode_ci NULL DEFAULT NULL,
  `nationality` varchar(255) CHARACTER SET utf8 COLLATE utf8_unicode_ci NULL DEFAULT NULL,
  `birth_certificate_no` varchar(30) CHARACTER SET utf8 COLLATE utf8_unicode_ci NULL DEFAULT NULL,
  `nid` varchar(20) CHARACTER SET utf8 COLLATE utf8_unicode_ci NULL DEFAULT NULL,
  `tin` varchar(20) CHARACTER SET utf8 COLLATE utf8_unicode_ci NULL DEFAULT NULL,
  `user_sign` varchar(100) CHARACTER SET utf8 COLLATE utf8_unicode_ci NULL DEFAULT NULL,
  `last_login` datetime(0) NULL DEFAULT NULL,
  `date_of_join` date NULL DEFAULT NULL,
  `date_of_confirmation` date NULL DEFAULT NULL,
  `is_reliever` tinyint(1) NULL DEFAULT 0,
  `reliever_to` int(10) NULL DEFAULT NULL,
  `reliever_start_datetime` datetime(0) NULL DEFAULT NULL,
  `reliever_end_datetime` datetime(0) NULL DEFAULT NULL,
  `bat_company_id` int(11) NULL DEFAULT NULL,
  `route_number` varchar(255) CHARACTER SET utf8 COLLATE utf8_unicode_ci NULL DEFAULT NULL,
  `bat_dpid` int(11) NULL DEFAULT NULL,
  `privilege_houses` varchar(255) CHARACTER SET utf8 COLLATE utf8_unicode_ci NULL DEFAULT NULL,
  `privilege_points` varchar(255) CHARACTER SET utf8 COLLATE utf8_unicode_ci NULL DEFAULT NULL,
  `designations_id` int(10) NULL DEFAULT NULL,
  `departments_id` int(10) NULL DEFAULT NULL,
  `branchs_id` int(10) NULL DEFAULT 1,
  `hr_emp_grades_id` int(10) NULL DEFAULT NULL,
  `hr_emp_units_id` int(10) NULL DEFAULT NULL,
  `hr_emp_categorys_id` int(10) NULL DEFAULT 1,
  `hr_emp_sections_id` int(10) NULL DEFAULT NULL,
  `line_manager_id` int(10) NULL DEFAULT NULL,
  `hr_working_shifts_id` int(2) NULL DEFAULT 1,
  `start_time` time(0) NULL DEFAULT '09:00:00',
  `end_time` time(0) NULL DEFAULT '18:00:00',
  `basic_salary` decimal(10, 2) NULL DEFAULT 0,
  `other_conveyance` tinyint(2) NULL DEFAULT 0,
  `pf_amount_employee` decimal(10, 2) NULL DEFAULT NULL,
  `pf_amount_company` decimal(10, 2) NULL DEFAULT 0,
  `gf_amount` decimal(10, 2) NULL DEFAULT 0,
  `insurance_amount` decimal(10, 2) NULL DEFAULT 0,
  `min_gross` decimal(10, 2) NULL DEFAULT 0,
  `applicable_date` date NULL DEFAULT NULL,
  `max_variable_salary` decimal(10, 2) NULL DEFAULT 0,
  `yearly_increment` decimal(4, 2) NULL DEFAULT 0,
  `ot_applicable` varchar(255) CHARACTER SET utf8 COLLATE utf8_unicode_ci NULL DEFAULT NULL,
  `pf_applicable` tinyint(1) NULL DEFAULT 0,
  `gf_applicable` tinyint(1) NULL DEFAULT 0,
  `insurance_applicable` tinyint(1) NULL DEFAULT 0,
  `late_deduction_applied` tinyint(1) NULL DEFAULT 0,
  `default_salary_applied` tinyint(1) NULL DEFAULT 1,
  `salary_disburse_type` varchar(255) CHARACTER SET utf8 COLLATE utf8_unicode_ci NULL DEFAULT NULL,
  `mfs_account_name_old` varchar(100) CHARACTER SET utf8 COLLATE utf8_unicode_ci NULL DEFAULT NULL,
  `salary_account_no` varchar(30) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL,
  `reference_user_id` int(10) NULL DEFAULT NULL,
  `geo_location_7_id` int(9) NOT NULL DEFAULT 1,
  `created_by` int(10) NULL DEFAULT NULL,
  `created_at` datetime(0) NULL DEFAULT NULL,
  `updated_at` timestamp(0) NULL DEFAULT NULL,
  `updated_by` int(11) NULL DEFAULT 0,
  `is_roaster` tinyint(4) NULL DEFAULT 0,
  `status` varchar(255) CHARACTER SET utf8 COLLATE utf8_unicode_ci NULL DEFAULT NULL,
  `working_type` varchar(255) CHARACTER SET utf8 COLLATE utf8_unicode_ci NULL DEFAULT NULL,
  `separation_date` date NULL DEFAULT NULL,
  `hr_separation_causes` varchar(255) CHARACTER SET utf8 COLLATE utf8_unicode_ci NULL DEFAULT NULL,
  `leave_policy_apply` tinyint(1) NULL DEFAULT 0,
  `is_transfer` tinyint(1) NULL DEFAULT NULL,
  `identity_type` varchar(255) CHARACTER SET utf8 COLLATE utf8_unicode_ci NULL DEFAULT NULL,
  `identity_number` varchar(155) CHARACTER SET utf8 COLLATE utf8_unicode_ci NULL DEFAULT NULL,
  `sub_designation` int(10) NULL DEFAULT NULL,
  `mfs_account_number` varchar(55) CHARACTER SET utf8 COLLATE utf8_unicode_ci NULL DEFAULT NULL,
  `mfs_account_name` varchar(255) CHARACTER SET utf8 COLLATE utf8_unicode_ci NULL DEFAULT NULL
) ENGINE = Columnstore CHARACTER SET = utf8 COLLATE = utf8_unicode_ci ROW_FORMAT = Dynamic;
"""
CREATE_retailer_comms_shown_fact_TABLE_IF_NOT_EXIST = """
    CREATE TABLE IF NOT EXISTS `retailer_comms_shown_fact`  (
      `id` int(20) NOT NULL,
      `dpid` int(20) NOT NULL,
      `route_id` int(20) NOT NULL,
      `retailer_id` int(30) NOT NULL,
      `retailer_code` varchar(50) CHARACTER SET latin1 COLLATE latin1_swedish_ci NOT NULL,
      `av_shown` tinyint(1) NOT NULL,
      `date` date NULL DEFAULT NULL
    ) ENGINE = Columnstore CHARACTER SET = latin1 COLLATE = latin1_swedish_ci ROW_FORMAT = Dynamic;

"""
