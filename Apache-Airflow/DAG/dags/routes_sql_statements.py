CREATE_routes_dimension_TABLE_IF_NOT_EXISTS = """
CREATE TABLE IF NOT EXISTS `routes_dimension` (
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
