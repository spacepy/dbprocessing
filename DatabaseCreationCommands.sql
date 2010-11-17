
-- DROP DATABASE rbsp;

CREATE DATABASE rbsp
  WITH OWNER = rbsp_owner
       ENCODING = 'UTF8'
       TABLESPACE = pg_default
       CONNECTION LIMIT = -1;
GRANT TEMPORARY ON DATABASE rbsp TO public;
GRANT ALL ON DATABASE rbsp TO rbsp_owner;


-- Schema: public

-- DROP SCHEMA public;

CREATE SCHEMA public
  AUTHORIZATION postgres;
GRANT ALL ON SCHEMA public TO postgres;
GRANT ALL ON SCHEMA public TO public;
COMMENT ON SCHEMA public IS 'Standard public schema';


-- Table: conc_code

-- DROP TABLE conc_code;

CREATE TABLE conc_code
(
  code_id bigint NOT NULL DEFAULT nextval('executable_codes_sc_id_seq'::regclass), -- Primary Key for this table
  filename character varying(50), -- Full code filename. Should adher to naming and versioning convention e.g. rbsp-a_C0-l0tol1_hope_v0.0.1
  relative_path character varying(50),
  code_start_date date, -- Date from which this version of code is valid
  code_end_date date, -- Date ubtil when this version of the code is valid
  code_desc character varying(20), -- functional identifier, e.g. REPT_L0toL1
  process_id integer NOT NULL, -- fk that points at a process
  interface_version smallint NOT NULL,
  quality_version smallint NOT NULL,
  revision_version smallint NOT NULL,
  active_code boolean NOT NULL DEFAULT false,
  date_written date, -- date the code was written (or added to DB)
  output_interface_version integer, -- output interface version, allows seperate tracking of input and output versions.
  CONSTRAINT software_codes_pk PRIMARY KEY (code_id),
  CONSTRAINT process_id_fk FOREIGN KEY (process_id)
      REFERENCES process (process_id) MATCH SIMPLE
      ON UPDATE NO ACTION ON DELETE NO ACTION
)
WITHOUT OIDS;
ALTER TABLE conc_code OWNER TO rbsp_owner;
GRANT ALL ON TABLE conc_code TO rbsp_owner;
GRANT SELECT, UPDATE, INSERT ON TABLE conc_code TO "RBSP_GRP";
GRANT SELECT, UPDATE, INSERT ON TABLE conc_code TO rbsp_user;
GRANT SELECT, UPDATE, INSERT, DELETE ON TABLE conc_code TO rbsp_ops;
COMMENT ON COLUMN conc_code.code_id IS 'Primary Key for this table';
COMMENT ON COLUMN conc_code.filename IS 'Full code filename. Should adher to naming and versioning convention e.g. rbsp-a_C0-l0tol1_hope_v0.0.1';
COMMENT ON COLUMN conc_code.code_start_date IS 'Date from which this version of code is valid';
COMMENT ON COLUMN conc_code.code_end_date IS 'Date ubtil when this version of the code is valid';
COMMENT ON COLUMN conc_code.code_desc IS 'functional identifier, e.g. REPT_L0toL1';
COMMENT ON COLUMN conc_code.process_id IS 'fk that points at a process
';
COMMENT ON COLUMN conc_code.date_written IS 'date the code was written (or added to DB)';
COMMENT ON COLUMN conc_code.output_interface_version IS 'output interface version, allows seperate tracking of input and output versions.';


-- Index: fki_process_id_fk

-- DROP INDEX fki_process_id_fk;

CREATE INDEX fki_process_id_fk
  ON conc_code
  USING btree
  (process_id);



  -- Table: conc_file

-- DROP TABLE conc_file;

CREATE TABLE conc_file
(
  file_id bigint NOT NULL DEFAULT nextval('datafiles_file_id_seq'::regclass), -- Primary Key for this table
  filename character varying(50) NOT NULL, -- File name only, no path info.
  utc_file_date date, -- Calendar date of the file, format: YYYYMMDD
  utc_start_time timestamp without time zone, -- YYYYMMDD HHMMSS
  utc_end_time timestamp without time zone, -- YYYYMMDD HHMMSS
  data_level real NOT NULL, -- 0,1,2,3,4,...
  check_date timestamp without time zone, -- Date and Time the file was last checked for consistency.  YYYYMMDD-HHMMSS
  interface_version integer NOT NULL, -- the XX part of version string vXX.YY.ZZ
  verbose_provenance character varying(500), -- Content of .info file, CDF version field
  quality_comment character varying(100),
  caveats character varying(20),
  release_number character varying(2), -- 01 to 99
  quality_version smallint NOT NULL, -- the YY part of version string vXX.YY.ZZ
  revision_version smallint NOT NULL, -- the ZZ part of version string vXX.YY.ZZ
  file_create_date timestamp without time zone DEFAULT ('now'::text)::timestamp without time zone, -- YYYYMMDD HHMMSS
  met_start_time bigint,
  met_stop_time bigint,
  exists_on_disk boolean NOT NULL,
  quality_checked boolean DEFAULT false, -- Data file checked for quality?
  product_id integer NOT NULL, -- FK to product table
  creation_date timestamp without time zone[] NOT NULL, -- date the file was created
  CONSTRAINT datafiles_pk PRIMARY KEY (file_id),
  CONSTRAINT product_id_fk FOREIGN KEY (product_id)
      REFERENCES product (product_id) MATCH SIMPLE
      ON UPDATE NO ACTION ON DELETE NO ACTION,
  CONSTRAINT filename_unique UNIQUE (filename) -- The filename must be unique
)
WITHOUT OIDS;
ALTER TABLE conc_file OWNER TO rbsp_owner;
GRANT ALL ON TABLE conc_file TO rbsp_owner;
GRANT SELECT, UPDATE, INSERT ON TABLE conc_file TO "RBSP_GRP";
GRANT SELECT, UPDATE, INSERT ON TABLE conc_file TO rbsp_user;
GRANT SELECT, UPDATE, INSERT, DELETE ON TABLE conc_file TO rbsp_ops;
COMMENT ON COLUMN conc_file.file_id IS 'Primary Key for this table';
COMMENT ON COLUMN conc_file.filename IS 'File name only, no path info. ';
COMMENT ON COLUMN conc_file.utc_file_date IS 'Calendar date of the file, format: YYYYMMDD';
COMMENT ON COLUMN conc_file.utc_start_time IS 'YYYYMMDD HHMMSS';
COMMENT ON COLUMN conc_file.utc_end_time IS 'YYYYMMDD HHMMSS';
COMMENT ON COLUMN conc_file.data_level IS '0,1,2,3,4,...';
COMMENT ON COLUMN conc_file.check_date IS 'Date and Time the file was last checked for consistency.  YYYYMMDD-HHMMSS';
COMMENT ON COLUMN conc_file.interface_version IS 'the XX part of version string vXX.YY.ZZ';
COMMENT ON COLUMN conc_file.verbose_provenance IS 'Content of .info file, CDF version field';
COMMENT ON COLUMN conc_file.release_number IS '01 to 99';
COMMENT ON COLUMN conc_file.quality_version IS 'the YY part of version string vXX.YY.ZZ';
COMMENT ON COLUMN conc_file.revision_version IS 'the ZZ part of version string vXX.YY.ZZ';
COMMENT ON COLUMN conc_file.file_create_date IS 'YYYYMMDD HHMMSS';
COMMENT ON COLUMN conc_file.quality_checked IS 'Data file checked for quality?';
COMMENT ON COLUMN conc_file.product_id IS 'FK to product table';
COMMENT ON COLUMN conc_file.creation_date IS 'date the file was created';

COMMENT ON CONSTRAINT filename_unique ON conc_file IS 'The filename must be unique';


-- Index: fki_product_id_fk

-- DROP INDEX fki_product_id_fk;

CREATE INDEX fki_product_id_fk
  ON conc_file
  USING btree
  (product_id);




  -- Table: filecodelink

-- DROP TABLE filecodelink;

CREATE TABLE filecodelink
(
  resulting_file bigint NOT NULL, -- File ID of the file with dependent executable code
  source_code bigint NOT NULL, -- The executable code ID of the code that produced this datafile
  CONSTRAINT filecodelink_pkey PRIMARY KEY (resulting_file, source_code),
  CONSTRAINT code_dependencies_d_code_id_fkey FOREIGN KEY (source_code)
      REFERENCES conc_code (code_id) MATCH SIMPLE
      ON UPDATE NO ACTION ON DELETE NO ACTION,
  CONSTRAINT code_dependencies_data_files_fk FOREIGN KEY (resulting_file)
      REFERENCES conc_file (file_id) MATCH SIMPLE
      ON UPDATE NO ACTION ON DELETE NO ACTION
)
WITHOUT OIDS;
ALTER TABLE filecodelink OWNER TO rbsp_owner;
COMMENT ON COLUMN filecodelink.resulting_file IS 'File ID of the file with dependent executable code';
COMMENT ON COLUMN filecodelink.source_code IS 'The executable code ID of the code that produced this datafile';



-- Table: filefilelink

-- DROP TABLE filefilelink;

CREATE TABLE filefilelink
(
  source_file bigint NOT NULL, -- A source file for a resulting file.
  resulting_file bigint NOT NULL, -- The resulting file that comes from a source file.
  CONSTRAINT filefilelink_pkey PRIMARY KEY (source_file, resulting_file),
  CONSTRAINT ffilefilelink_dependent_f_id_fk FOREIGN KEY (source_file)
      REFERENCES conc_file (file_id) MATCH SIMPLE
      ON UPDATE NO ACTION ON DELETE NO ACTION,
  CONSTRAINT ffilefilelink_f_id_fk FOREIGN KEY (resulting_file)
      REFERENCES conc_file (file_id) MATCH SIMPLE
      ON UPDATE NO ACTION ON DELETE NO ACTION
)
WITHOUT OIDS;
ALTER TABLE filefilelink OWNER TO rbsp_owner;
COMMENT ON COLUMN filefilelink.source_file IS 'A source file for a resulting file.';
COMMENT ON COLUMN filefilelink.resulting_file IS 'The resulting file that comes from a source file.';



-- Table: instrument

-- DROP TABLE instrument;

CREATE TABLE instrument
(
  instrument_id integer NOT NULL DEFAULT nextval('data_sources_ds_id_seq'::regclass), -- Primary Key for this table
  instrument_name character varying(20) NOT NULL, -- names of the instruments on the spacecraft
  satellite_id integer NOT NULL,
  CONSTRAINT data_sources_pk PRIMARY KEY (instrument_id),
  CONSTRAINT satellite_id_fk FOREIGN KEY (satellite_id)
      REFERENCES satellite (satellite_id) MATCH SIMPLE
      ON UPDATE NO ACTION ON DELETE NO ACTION
)
WITHOUT OIDS;
ALTER TABLE instrument OWNER TO rbsp_owner;
GRANT ALL ON TABLE instrument TO rbsp_owner;
GRANT SELECT, UPDATE, INSERT ON TABLE instrument TO "RBSP_GRP";
GRANT SELECT, UPDATE, INSERT ON TABLE instrument TO rbsp_user;
GRANT SELECT, UPDATE, INSERT, DELETE ON TABLE instrument TO rbsp_ops;
COMMENT ON COLUMN instrument.instrument_id IS 'Primary Key for this table';
COMMENT ON COLUMN instrument.instrument_name IS 'names of the instruments on the spacecraft';


-- Index: fki_satellite_id_fk

-- DROP INDEX fki_satellite_id_fk;

CREATE INDEX fki_satellite_id_fk
  ON instrument
  USING btree
  (satellite_id);



  -- Table: instrumentproductlink

-- DROP TABLE instrumentproductlink;

CREATE TABLE instrumentproductlink
(
  instrument_id integer,
  product_id integer,
  CONSTRAINT instrument_id_fk FOREIGN KEY (instrument_id)
      REFERENCES instrument (instrument_id) MATCH SIMPLE
      ON UPDATE NO ACTION ON DELETE NO ACTION,
  CONSTRAINT product_id_fk FOREIGN KEY (product_id)
      REFERENCES product (product_id) MATCH SIMPLE
      ON UPDATE NO ACTION ON DELETE NO ACTION
)
WITHOUT OIDS;
ALTER TABLE instrumentproductlink OWNER TO rbsp_owner;

-- Index: fki_instrument_id_fk

-- DROP INDEX fki_instrument_id_fk;

CREATE INDEX fki_instrument_id_fk
  ON instrumentproductlink
  USING btree
  (instrument_id);

-- Index: fki_product_id_fk2

-- DROP INDEX fki_product_id_fk2;

CREATE INDEX fki_product_id_fk2
  ON instrumentproductlink
  USING btree
  (product_id);





  -- Table: logging

-- DROP TABLE logging;

CREATE TABLE logging
(
  logging_id bigint NOT NULL DEFAULT nextval('processing_p_id_seq'::regclass),
  currently_processing boolean NOT NULL DEFAULT false, -- Flag to keep track if a processing script is currently running.  If one is running dont allow another to start.  -- could be done in unix also with pid
  pid integer, -- Process ID of the proicessing script running on the database
  processing_start_time timestamp without time zone NOT NULL, -- date and time that processing started
  processing_end_time timestamp without time zone, -- date and time that the processing ended, null until it ended.
  "comment" text,
  mission_id integer,
  "user" text NOT NULL, -- user that started the processing
  hostname text NOT NULL, -- The hpstname that initiated the processing
  CONSTRAINT processing_pk PRIMARY KEY (logging_id),
  CONSTRAINT mission_id_processing FOREIGN KEY (mission_id)
      REFERENCES mission (mission_id) MATCH SIMPLE
      ON UPDATE NO ACTION ON DELETE NO ACTION,
  CONSTRAINT end_start_check CHECK (processing_start_time < processing_end_time) -- end times must be after start times
)
WITHOUT OIDS;
ALTER TABLE logging OWNER TO rbsp_owner;
COMMENT ON TABLE logging IS 'Table to keep track of processing done on the database, may or may not use this.  -- BAL';
COMMENT ON COLUMN logging.currently_processing IS 'Flag to keep track if a processing script is currently running.  If one is running dont allow another to start.  -- could be done in unix also with pid
';
COMMENT ON COLUMN logging.pid IS 'Process ID of the proicessing script running on the database
';
COMMENT ON COLUMN logging.processing_start_time IS 'date and time that processing started';
COMMENT ON COLUMN logging.processing_end_time IS 'date and time that the processing ended, null until it ended.
';
COMMENT ON COLUMN logging."user" IS 'user that started the processing';
COMMENT ON COLUMN logging.hostname IS 'The hpstname that initiated the processing';

COMMENT ON CONSTRAINT end_start_check ON logging IS 'end times must be after start times';





-- Table: logging_file

-- DROP TABLE logging_file;

CREATE TABLE logging_file
(
  logging_file_id bigint NOT NULL DEFAULT nextval('processing_files_seq'::regclass),
  logging_id bigint NOT NULL,
  file_id bigint,
  code_id integer,
  comments text,
  CONSTRAINT processing_files_pk PRIMARY KEY (logging_file_id),
  CONSTRAINT processing_files_data_files_fk FOREIGN KEY (file_id)
      REFERENCES conc_file (file_id) MATCH SIMPLE
      ON UPDATE NO ACTION ON DELETE NO ACTION,
  CONSTRAINT processing_files_executable_codes_fk FOREIGN KEY (code_id)
      REFERENCES conc_code (code_id) MATCH SIMPLE
      ON UPDATE NO ACTION ON DELETE NO ACTION,
  CONSTRAINT processing_files_processing_fk FOREIGN KEY (logging_id)
      REFERENCES logging (logging_id) MATCH SIMPLE
      ON UPDATE NO ACTION ON DELETE NO ACTION
)
WITHOUT OIDS;
ALTER TABLE logging_file OWNER TO rbsp_owner;
COMMENT ON TABLE logging_file IS 'table to keep track of which files were processed during a db processing run. ';




-- Table: mission

-- DROP TABLE mission;

CREATE TABLE mission
(
  mission_id integer NOT NULL DEFAULT nextval('missions_mission_id_seq'::regclass),
  mission_name character varying(20) NOT NULL, -- Name of overall mission: RBSP, LANL-GEO
  rootdir character varying(50) NOT NULL, -- Root of directory tree for this mission....
  CONSTRAINT missions_pk PRIMARY KEY (mission_id)
)
WITHOUT OIDS;
ALTER TABLE mission OWNER TO rbsp_owner;
GRANT ALL ON TABLE mission TO rbsp_owner;
GRANT SELECT, UPDATE, INSERT ON TABLE mission TO "RBSP_GRP";
GRANT SELECT, UPDATE, INSERT ON TABLE mission TO rbsp_user;
GRANT SELECT, UPDATE, INSERT, DELETE ON TABLE mission TO rbsp_ops;
COMMENT ON COLUMN mission.mission_name IS 'Name of overall mission: RBSP, LANL-GEO';
COMMENT ON COLUMN mission.rootdir IS 'Root of directory tree for this mission.
rootdir/data    is the root for all datafiles
rootdir/proc    is the root for process codes';



-- Table: process

-- DROP TABLE process;

CREATE TABLE process
(
  process_id integer NOT NULL DEFAULT nextval('processes_p_id_seq'::regclass),
  process_name character varying(20) NOT NULL,
  output_product integer NOT NULL,
  extra_params text, -- Extra paramaeters needed for this process to run
  super_process_id integer, -- Allows for subprocesses, may not be used
  CONSTRAINT processes_pk PRIMARY KEY (process_id),
  CONSTRAINT output_product_fk FOREIGN KEY (output_product)
      REFERENCES product (product_id) MATCH SIMPLE
      ON UPDATE NO ACTION ON DELETE NO ACTION
)
WITHOUT OIDS;
ALTER TABLE process OWNER TO rbsp_owner;
GRANT ALL ON TABLE process TO rbsp_owner;
GRANT SELECT, UPDATE, INSERT ON TABLE process TO "RBSP_GRP";
GRANT SELECT, UPDATE, INSERT ON TABLE process TO rbsp_user;
GRANT SELECT, UPDATE, INSERT, DELETE ON TABLE process TO rbsp_ops;
COMMENT ON COLUMN process.extra_params IS 'Extra paramaeters needed for this process to run';
COMMENT ON COLUMN process.super_process_id IS 'Allows for subprocesses, may not be used';


-- Index: fki_output_product_fk

-- DROP INDEX fki_output_product_fk;

CREATE INDEX fki_output_product_fk
  ON process
  USING btree
  (output_product);




  -- Table: product

-- DROP TABLE product;

CREATE TABLE product
(
  product_id integer NOT NULL DEFAULT nextval('data_products_dp_id_seq'::regclass), -- Primary Key for this table
  product_name character varying(30) NOT NULL, -- A name describing the general class of this data product, e.g. "MagEIS-L3-pitch"
  instrument_id integer NOT NULL, -- Data source id for the data source for this product.
  relative_path character varying(50), -- Relative path to the data directory with respect to mission rootdir.
  super_product_id integer, -- Allows for subproducts, maybe not used
  CONSTRAINT data_products_pk PRIMARY KEY (product_id)
)
WITHOUT OIDS;
ALTER TABLE product OWNER TO rbsp_owner;
COMMENT ON COLUMN product.product_id IS 'Primary Key for this table
';
COMMENT ON COLUMN product.product_name IS 'A name describing the general class of this data product, e.g. "MagEIS-L3-pitch"';
COMMENT ON COLUMN product.instrument_id IS 'Data source id for the data source for this product.
';
COMMENT ON COLUMN product.relative_path IS 'Relative path to the data directory with respect to mission rootdir. ';
COMMENT ON COLUMN product.super_product_id IS 'Allows for subproducts, maybe not used
';





-- Table: productprocesslink

-- DROP TABLE productprocesslink;

CREATE TABLE productprocesslink
(
  process_id integer NOT NULL, -- process ID for this data_type
  product_id integer NOT NULL, -- Data Product Index of data product that is either an inpout or output of process p_id
  CONSTRAINT productprocesslink_pkey PRIMARY KEY (process_id, product_id),
  CONSTRAINT file_processes_data_products_fk FOREIGN KEY (product_id)
      REFERENCES product (product_id) MATCH SIMPLE
      ON UPDATE NO ACTION ON DELETE NO ACTION,
  CONSTRAINT file_processes_processes_fk FOREIGN KEY (process_id)
      REFERENCES process (process_id) MATCH SIMPLE
      ON UPDATE NO ACTION ON DELETE NO ACTION
)
WITHOUT OIDS;
ALTER TABLE productprocesslink OWNER TO rbsp_owner;
GRANT ALL ON TABLE productprocesslink TO rbsp_owner;
GRANT SELECT, UPDATE, INSERT ON TABLE productprocesslink TO "RBSP_GRP";
GRANT SELECT, UPDATE, INSERT ON TABLE productprocesslink TO rbsp_user;
GRANT SELECT, UPDATE, INSERT, DELETE ON TABLE productprocesslink TO rbsp_ops;
COMMENT ON COLUMN productprocesslink.process_id IS 'process ID for this data_type';
COMMENT ON COLUMN productprocesslink.product_id IS 'Data Product Index of data product that is either an inpout or output of process p_id';




-- Table: satellite

-- DROP TABLE satellite;

CREATE TABLE satellite
(
  satellite_id integer NOT NULL DEFAULT nextval('satellites_s_id_seq'::regclass), -- primary key
  satellite_name character varying(20) NOT NULL, -- For RBSP: A or B
  mission_id integer NOT NULL,
  CONSTRAINT satellites_pk PRIMARY KEY (satellite_id),
  CONSTRAINT satellites_missions_fk FOREIGN KEY (mission_id)
      REFERENCES mission (mission_id) MATCH SIMPLE
      ON UPDATE NO ACTION ON DELETE NO ACTION
)
WITHOUT OIDS;
ALTER TABLE satellite OWNER TO rbsp_owner;
GRANT ALL ON TABLE satellite TO rbsp_owner;
GRANT SELECT, UPDATE, INSERT ON TABLE satellite TO "RBSP_GRP";
GRANT SELECT, UPDATE, INSERT ON TABLE satellite TO rbsp_user;
GRANT SELECT, UPDATE, INSERT, DELETE ON TABLE satellite TO rbsp_ops;
COMMENT ON COLUMN satellite.satellite_id IS 'primary key';
COMMENT ON COLUMN satellite.satellite_name IS 'For RBSP: A or B';

