"""MySQL tables used by the optional WTStat all mirror."""

SCHEMA = (
    """CREATE TABLE IF NOT EXISTS `replays` (
      `sid` VARCHAR(16) PRIMARY KEY,
      `start_utc` VARCHAR(40), `map` TEXT, `mode` VARCHAR(32),
      `group_name` VARCHAR(16), `last_part` INT, `status` VARCHAR(32) NOT NULL,
      `error` TEXT, `player_count` INT, `match_br` DOUBLE,
      `processed_at` DOUBLE NOT NULL, `team_br_json` LONGTEXT,
      `br_ceiling_tick` INT
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4""",
    """CREATE TABLE IF NOT EXISTS `matches` (
      `sid` VARCHAR(16) PRIMARY KEY,
      `start_utc` VARCHAR(40), `map` TEXT, `mode` VARCHAR(16),
      `player_count` INT, `last_part` INT, `processed_at` DOUBLE,
      `match_br_min` DOUBLE, `match_br_max` DOUBLE,
      `team1_br_min` DOUBLE, `team1_br_max` DOUBLE,
      `team1_average_squad_br` DOUBLE,
      `team2_br_min` DOUBLE, `team2_br_max` DOUBLE,
      `team2_average_squad_br` DOUBLE
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4""",
    """CREATE TABLE IF NOT EXISTS `samples` (
      `sid` VARCHAR(16) NOT NULL, `uid` VARCHAR(64) NOT NULL,
      `player_name` TEXT, `country` VARCHAR(128),
      `country_code` CHAR(2), `country_name` VARCHAR(64), `squad_br` DOUBLE,
      `match_br` DOUBLE, `mode` VARCHAR(16) NOT NULL,
      `squad_countries` LONGTEXT, `squad_country_codes` LONGTEXT,
      `squad_country_names` LONGTEXT, `party_key` TEXT,
      `configured_vehicles` LONGTEXT, `stats_json` LONGTEXT, `team` INT,
      PRIMARY KEY (`sid`,`uid`), INDEX `samples_uid` (`uid`),
      INDEX `samples_filter` (`mode`,`squad_br`,`country`)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4""",
)
