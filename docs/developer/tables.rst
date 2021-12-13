######
Tables
######
================================== =============================================
:sql:table:`code`                  code summary
:sql:table:`file`                  file summary
:sql:table:`filecodelink`          filecodelink summary
:sql:table:`filefilelink`          filefilelink summary
:sql:table:`inspector`             inspector summary
:sql:table:`instrument`            instrument summary
:sql:table:`instrumentproductlink` instrumentproductlink summary
:sql:table:`logging`               logging summary
:sql:table:`logging_file`          logging_file summary
:sql:table:`mission`               mission summary
:sql:table:`process`               process summary
:sql:table:`processqueue`          processqueue summary
:sql:table:`product`               product summary
:sql:table:`productprocesslink`    productprocesslink summary
:sql:table:`release`               release summary
:sql:table:`satellite`             satellite summary
:sql:table:`unixtime`              unixtime summary
================================== =============================================

.. sql:table:: code

.. sql:column:: code_id

   (:py:class:`~sqlalchemy.types.Integer`,
   :py:class:`PK <sqlalchemy.schema.PrimaryKeyConstraint>`,
   :py:obj:`NOT NULL <sqlalchemy.schema.Column.params.nullable>`)

.. sql:column:: filename

   (:py:class:`~sqlalchemy.types.String`,
   :py:obj:`NOT NULL <sqlalchemy.schema.Column.params.nullable>`)

.. sql:column:: relative_path

   (:py:class:`~sqlalchemy.types.String`,
   :py:obj:`NOT NULL <sqlalchemy.schema.Column.params.nullable>`)

.. sql:column:: code_start_date

   (:py:class:`~sqlalchemy.types.Date`,
   :py:obj:`NOT NULL <sqlalchemy.schema.Column.params.nullable>`)

.. sql:column:: code_stop_date

   (:py:class:`~sqlalchemy.types.Date`,
   :py:obj:`NOT NULL <sqlalchemy.schema.Column.params.nullable>`)

.. sql:column:: code_description

   (:py:class:`~sqlalchemy.types.Text`,
   :py:obj:`NOT NULL <sqlalchemy.schema.Column.params.nullable>`)

.. sql:column:: process_id

   (:py:class:`~sqlalchemy.types.Integer`,
   :py:obj:`NOT NULL <sqlalchemy.schema.Column.params.nullable>`,
   :py:class:`FK <sqlalchemy.schema.ForeignKeyConstraint>`
   :sql:column:`process.process_id`)

.. sql:column:: interface_version

   (:py:class:`~sqlalchemy.types.SmallInteger`,
   :py:obj:`NOT NULL <sqlalchemy.schema.Column.params.nullable>`)

.. sql:column:: quality_version

   (:py:class:`~sqlalchemy.types.SmallInteger`,
   :py:obj:`NOT NULL <sqlalchemy.schema.Column.params.nullable>`)

.. sql:column:: revision_version

   (:py:class:`~sqlalchemy.types.SmallInteger`,
   :py:obj:`NOT NULL <sqlalchemy.schema.Column.params.nullable>`)

.. sql:column:: output_interface_version

   (:py:class:`~sqlalchemy.types.SmallInteger`,
   :py:obj:`NOT NULL <sqlalchemy.schema.Column.params.nullable>`)

.. sql:column:: active_code

   (:py:class:`~sqlalchemy.types.Boolean`,
   :py:obj:`NOT NULL <sqlalchemy.schema.Column.params.nullable>`)

.. sql:column:: date_written

   (:py:class:`~sqlalchemy.types.Date`,
   :py:obj:`NOT NULL <sqlalchemy.schema.Column.params.nullable>`)

.. sql:column:: shasum

   (:py:class:`~sqlalchemy.types.String`)

.. sql:column:: newest_version

   (:py:class:`~sqlalchemy.types.Boolean`,
   :py:obj:`NOT NULL <sqlalchemy.schema.Column.params.nullable>`)

.. sql:column:: arguments

   (:py:class:`~sqlalchemy.types.Text`)

.. sql:column:: ram

   (:py:class:`~sqlalchemy.types.Float`)

.. sql:column:: cpu

   (:py:class:`~sqlalchemy.types.SmallInteger`)

.. sql:table:: file

.. sql:column:: file_id

   (:py:class:`~sqlalchemy.types.Integer`,
   :py:class:`PK <sqlalchemy.schema.PrimaryKeyConstraint>`,
   :py:obj:`NOT NULL <sqlalchemy.schema.Column.params.nullable>`)

.. sql:column:: filename

   (:py:class:`~sqlalchemy.types.String`,
   :py:obj:`NOT NULL <sqlalchemy.schema.Column.params.nullable>`)

.. sql:column:: utc_file_date

   (:py:class:`~sqlalchemy.types.Date`)

.. sql:column:: utc_start_time

   (:py:class:`~sqlalchemy.types.DateTime`)

.. sql:column:: utc_stop_time

   (:py:class:`~sqlalchemy.types.DateTime`)

.. sql:column:: data_level

   (:py:class:`~sqlalchemy.types.Float`,
   :py:obj:`NOT NULL <sqlalchemy.schema.Column.params.nullable>`)

.. sql:column:: interface_version

   (:py:class:`~sqlalchemy.types.SmallInteger`,
   :py:obj:`NOT NULL <sqlalchemy.schema.Column.params.nullable>`)

.. sql:column:: quality_version

   (:py:class:`~sqlalchemy.types.SmallInteger`,
   :py:obj:`NOT NULL <sqlalchemy.schema.Column.params.nullable>`)

.. sql:column:: revision_version

   (:py:class:`~sqlalchemy.types.SmallInteger`,
   :py:obj:`NOT NULL <sqlalchemy.schema.Column.params.nullable>`)

.. sql:column:: verbose_provenance

   (:py:class:`~sqlalchemy.types.Text`)

.. sql:column:: check_date

   (:py:class:`~sqlalchemy.types.DateTime`)

.. sql:column:: quality_comment

   (:py:class:`~sqlalchemy.types.Text`)

.. sql:column:: caveats

   (:py:class:`~sqlalchemy.types.Text`)

.. sql:column:: file_create_date

   (:py:class:`~sqlalchemy.types.DateTime`,
   :py:obj:`NOT NULL <sqlalchemy.schema.Column.params.nullable>`)

.. sql:column:: met_start_time

   (:py:class:`~sqlalchemy.types.Float`)

.. sql:column:: met_stop_time

   (:py:class:`~sqlalchemy.types.Float`)

.. sql:column:: exists_on_disk

   (:py:class:`~sqlalchemy.types.Boolean`,
   :py:obj:`NOT NULL <sqlalchemy.schema.Column.params.nullable>`)

.. sql:column:: quality_checked

   (:py:class:`~sqlalchemy.types.Boolean`)

.. sql:column:: product_id

   (:py:class:`~sqlalchemy.types.Integer`,
   :py:obj:`NOT NULL <sqlalchemy.schema.Column.params.nullable>`,
   :py:class:`FK <sqlalchemy.schema.ForeignKeyConstraint>`
   :sql:column:`product.product_id`)

.. sql:column:: shasum

   (:py:class:`~sqlalchemy.types.String`)

.. sql:column:: process_keywords

   (:py:class:`~sqlalchemy.types.Text`)

.. sql:table:: filecodelink

.. sql:column:: resulting_file

   (:py:class:`~sqlalchemy.types.Integer`,
   :py:obj:`NOT NULL <sqlalchemy.schema.Column.params.nullable>`,
   :py:class:`FK <sqlalchemy.schema.ForeignKeyConstraint>`
   :sql:column:`file.file_id`)

.. sql:column:: source_code

   (:py:class:`~sqlalchemy.types.Integer`,
   :py:obj:`NOT NULL <sqlalchemy.schema.Column.params.nullable>`,
   :py:class:`FK <sqlalchemy.schema.ForeignKeyConstraint>`
   :sql:column:`code.code_id`)

.. sql:table:: filefilelink

.. sql:column:: source_file

   (:py:class:`~sqlalchemy.types.Integer`,
   :py:obj:`NOT NULL <sqlalchemy.schema.Column.params.nullable>`,
   :py:class:`FK <sqlalchemy.schema.ForeignKeyConstraint>`
   :sql:column:`file.file_id`)

.. sql:column:: resulting_file

   (:py:class:`~sqlalchemy.types.Integer`,
   :py:obj:`NOT NULL <sqlalchemy.schema.Column.params.nullable>`,
   :py:class:`FK <sqlalchemy.schema.ForeignKeyConstraint>`
   :sql:column:`file.file_id`)

.. sql:table:: inspector

.. sql:column:: inspector_id

   (:py:class:`~sqlalchemy.types.Integer`,
   :py:class:`PK <sqlalchemy.schema.PrimaryKeyConstraint>`,
   :py:obj:`NOT NULL <sqlalchemy.schema.Column.params.nullable>`)

.. sql:column:: filename

   (:py:class:`~sqlalchemy.types.String`,
   :py:obj:`NOT NULL <sqlalchemy.schema.Column.params.nullable>`)

.. sql:column:: relative_path

   (:py:class:`~sqlalchemy.types.String`,
   :py:obj:`NOT NULL <sqlalchemy.schema.Column.params.nullable>`)

.. sql:column:: description

   (:py:class:`~sqlalchemy.types.Text`,
   :py:obj:`NOT NULL <sqlalchemy.schema.Column.params.nullable>`)

.. sql:column:: interface_version

   (:py:class:`~sqlalchemy.types.SmallInteger`,
   :py:obj:`NOT NULL <sqlalchemy.schema.Column.params.nullable>`)

.. sql:column:: quality_version

   (:py:class:`~sqlalchemy.types.SmallInteger`,
   :py:obj:`NOT NULL <sqlalchemy.schema.Column.params.nullable>`)

.. sql:column:: revision_version

   (:py:class:`~sqlalchemy.types.SmallInteger`,
   :py:obj:`NOT NULL <sqlalchemy.schema.Column.params.nullable>`)

.. sql:column:: output_interface_version

   (:py:class:`~sqlalchemy.types.SmallInteger`,
   :py:obj:`NOT NULL <sqlalchemy.schema.Column.params.nullable>`)

.. sql:column:: active_code

   (:py:class:`~sqlalchemy.types.Boolean`,
   :py:obj:`NOT NULL <sqlalchemy.schema.Column.params.nullable>`)

.. sql:column:: date_written

   (:py:class:`~sqlalchemy.types.Date`,
   :py:obj:`NOT NULL <sqlalchemy.schema.Column.params.nullable>`)

.. sql:column:: shasum

   (:py:class:`~sqlalchemy.types.String`)

.. sql:column:: newest_version

   (:py:class:`~sqlalchemy.types.Boolean`,
   :py:obj:`NOT NULL <sqlalchemy.schema.Column.params.nullable>`)

.. sql:column:: arguments

   (:py:class:`~sqlalchemy.types.Text`)

.. sql:column:: product

   (:py:class:`~sqlalchemy.types.Integer`,
   :py:obj:`NOT NULL <sqlalchemy.schema.Column.params.nullable>`,
   :py:class:`FK <sqlalchemy.schema.ForeignKeyConstraint>`
   :sql:column:`product.product_id`)

.. sql:table:: instrument

.. sql:column:: instrument_id

   (:py:class:`~sqlalchemy.types.Integer`,
   :py:class:`PK <sqlalchemy.schema.PrimaryKeyConstraint>`,
   :py:obj:`NOT NULL <sqlalchemy.schema.Column.params.nullable>`)

.. sql:column:: instrument_name

   (:py:class:`~sqlalchemy.types.String`,
   :py:obj:`NOT NULL <sqlalchemy.schema.Column.params.nullable>`)

.. sql:column:: satellite_id

   (:py:class:`~sqlalchemy.types.Integer`,
   :py:obj:`NOT NULL <sqlalchemy.schema.Column.params.nullable>`,
   :py:class:`FK <sqlalchemy.schema.ForeignKeyConstraint>`
   :sql:column:`satellite.satellite_id`)

.. sql:table:: instrumentproductlink

.. sql:column:: instrument_id

   (:py:class:`~sqlalchemy.types.Integer`,
   :py:obj:`NOT NULL <sqlalchemy.schema.Column.params.nullable>`,
   :py:class:`FK <sqlalchemy.schema.ForeignKeyConstraint>`
   :sql:column:`instrument.instrument_id`)

.. sql:column:: product_id

   (:py:class:`~sqlalchemy.types.Integer`,
   :py:obj:`NOT NULL <sqlalchemy.schema.Column.params.nullable>`,
   :py:class:`FK <sqlalchemy.schema.ForeignKeyConstraint>`
   :sql:column:`product.product_id`)

.. sql:table:: logging

.. sql:column:: logging_id

   (:py:class:`~sqlalchemy.types.Integer`,
   :py:class:`PK <sqlalchemy.schema.PrimaryKeyConstraint>`,
   :py:obj:`NOT NULL <sqlalchemy.schema.Column.params.nullable>`)

.. sql:column:: currently_processing

   (:py:class:`~sqlalchemy.types.Boolean`,
   :py:obj:`NOT NULL <sqlalchemy.schema.Column.params.nullable>`)

.. sql:column:: pid

   (:py:class:`~sqlalchemy.types.Integer`)

.. sql:column:: processing_start_time

   (:py:class:`~sqlalchemy.types.DateTime`,
   :py:obj:`NOT NULL <sqlalchemy.schema.Column.params.nullable>`)

.. sql:column:: processing_end_time

   (:py:class:`~sqlalchemy.types.DateTime`)

.. sql:column:: comment

   (:py:class:`~sqlalchemy.types.Text`)

.. sql:column:: mission_id

   (:py:class:`~sqlalchemy.types.Integer`,
   :py:obj:`NOT NULL <sqlalchemy.schema.Column.params.nullable>`,
   :py:class:`FK <sqlalchemy.schema.ForeignKeyConstraint>`
   :sql:column:`mission.mission_id`)

.. sql:column:: user

   (:py:class:`~sqlalchemy.types.String`,
   :py:obj:`NOT NULL <sqlalchemy.schema.Column.params.nullable>`)

.. sql:column:: hostname

   (:py:class:`~sqlalchemy.types.String`,
   :py:obj:`NOT NULL <sqlalchemy.schema.Column.params.nullable>`)

.. sql:table:: logging_file

.. sql:column:: logging_file_id

   (:py:class:`~sqlalchemy.types.Integer`,
   :py:class:`PK <sqlalchemy.schema.PrimaryKeyConstraint>`,
   :py:obj:`NOT NULL <sqlalchemy.schema.Column.params.nullable>`)

.. sql:column:: logging_id

   (:py:class:`~sqlalchemy.types.Integer`,
   :py:obj:`NOT NULL <sqlalchemy.schema.Column.params.nullable>`,
   :py:class:`FK <sqlalchemy.schema.ForeignKeyConstraint>`
   :sql:column:`logging.logging_id`)

.. sql:column:: file_id

   (:py:class:`~sqlalchemy.types.Integer`,
   :py:obj:`NOT NULL <sqlalchemy.schema.Column.params.nullable>`,
   :py:class:`FK <sqlalchemy.schema.ForeignKeyConstraint>`
   :sql:column:`file.file_id`)

.. sql:column:: code_id

   (:py:class:`~sqlalchemy.types.Integer`,
   :py:obj:`NOT NULL <sqlalchemy.schema.Column.params.nullable>`,
   :py:class:`FK <sqlalchemy.schema.ForeignKeyConstraint>`
   :sql:column:`code.code_id`)

.. sql:column:: comments

   (:py:class:`~sqlalchemy.types.Text`)

.. sql:table:: mission

.. sql:column:: mission_id

   (:py:class:`~sqlalchemy.types.Integer`,
   :py:class:`PK <sqlalchemy.schema.PrimaryKeyConstraint>`,
   :py:obj:`NOT NULL <sqlalchemy.schema.Column.params.nullable>`)

.. sql:column:: mission_name

   (:py:class:`~sqlalchemy.types.String`,
   :py:obj:`NOT NULL <sqlalchemy.schema.Column.params.nullable>`)

.. sql:column:: rootdir

   (:py:class:`~sqlalchemy.types.String`,
   :py:obj:`NOT NULL <sqlalchemy.schema.Column.params.nullable>`)

.. sql:column:: incoming_dir

   (:py:class:`~sqlalchemy.types.String`,
   :py:obj:`NOT NULL <sqlalchemy.schema.Column.params.nullable>`)

.. sql:column:: codedir

   (:py:class:`~sqlalchemy.types.String`)

.. sql:column:: inspectordir

   (:py:class:`~sqlalchemy.types.String`)

.. sql:column:: errordir

   (:py:class:`~sqlalchemy.types.String`)

.. sql:table:: process

.. sql:column:: process_id

   (:py:class:`~sqlalchemy.types.Integer`,
   :py:class:`PK <sqlalchemy.schema.PrimaryKeyConstraint>`,
   :py:obj:`NOT NULL <sqlalchemy.schema.Column.params.nullable>`)

.. sql:column:: process_name

   (:py:class:`~sqlalchemy.types.String`,
   :py:obj:`NOT NULL <sqlalchemy.schema.Column.params.nullable>`)

.. sql:column:: output_product

   (:py:class:`~sqlalchemy.types.Integer`,
   :py:class:`FK <sqlalchemy.schema.ForeignKeyConstraint>`
   :sql:column:`product.product_id`)

.. sql:column:: output_timebase

   (:py:class:`~sqlalchemy.types.String`)

.. sql:column:: extra_params

   (:py:class:`~sqlalchemy.types.Text`)

.. sql:table:: processqueue

.. sql:column:: file_id

   (:py:class:`~sqlalchemy.types.Integer`,
   :py:class:`PK <sqlalchemy.schema.PrimaryKeyConstraint>`,
   :py:obj:`NOT NULL <sqlalchemy.schema.Column.params.nullable>`,
   :py:class:`FK <sqlalchemy.schema.ForeignKeyConstraint>`
   :sql:column:`file.file_id`)

.. sql:column:: version_bump

   (:py:class:`~sqlalchemy.types.SmallInteger`)

.. sql:table:: product

.. sql:column:: product_id

   (:py:class:`~sqlalchemy.types.Integer`,
   :py:class:`PK <sqlalchemy.schema.PrimaryKeyConstraint>`,
   :py:obj:`NOT NULL <sqlalchemy.schema.Column.params.nullable>`)

.. sql:column:: product_name

   (:py:class:`~sqlalchemy.types.String`,
   :py:obj:`NOT NULL <sqlalchemy.schema.Column.params.nullable>`)

.. sql:column:: instrument_id

   (:py:class:`~sqlalchemy.types.Integer`,
   :py:obj:`NOT NULL <sqlalchemy.schema.Column.params.nullable>`,
   :py:class:`FK <sqlalchemy.schema.ForeignKeyConstraint>`
   :sql:column:`instrument.instrument_id`)

.. sql:column:: relative_path

   (:py:class:`~sqlalchemy.types.String`,
   :py:obj:`NOT NULL <sqlalchemy.schema.Column.params.nullable>`)

.. sql:column:: level

   (:py:class:`~sqlalchemy.types.Float`,
   :py:obj:`NOT NULL <sqlalchemy.schema.Column.params.nullable>`)

.. sql:column:: format

   (:py:class:`~sqlalchemy.types.Text`,
   :py:obj:`NOT NULL <sqlalchemy.schema.Column.params.nullable>`)

.. sql:column:: product_description

   (:py:class:`~sqlalchemy.types.Text`)

.. sql:table:: productprocesslink

.. sql:column:: process_id

   (:py:class:`~sqlalchemy.types.Integer`,
   :py:obj:`NOT NULL <sqlalchemy.schema.Column.params.nullable>`,
   :py:class:`FK <sqlalchemy.schema.ForeignKeyConstraint>`
   :sql:column:`process.process_id`)

.. sql:column:: input_product_id

   (:py:class:`~sqlalchemy.types.Integer`,
   :py:obj:`NOT NULL <sqlalchemy.schema.Column.params.nullable>`,
   :py:class:`FK <sqlalchemy.schema.ForeignKeyConstraint>`
   :sql:column:`product.product_id`)

.. sql:column:: optional

   (:py:class:`~sqlalchemy.types.Boolean`,
   :py:obj:`NOT NULL <sqlalchemy.schema.Column.params.nullable>`)

.. sql:column:: yesterday

   (:py:class:`~sqlalchemy.types.Integer`,
   :py:obj:`NOT NULL <sqlalchemy.schema.Column.params.nullable>`)

.. sql:column:: tomorrow

   (:py:class:`~sqlalchemy.types.Integer`,
   :py:obj:`NOT NULL <sqlalchemy.schema.Column.params.nullable>`)

.. sql:table:: release

.. sql:column:: file_id

   (:py:class:`~sqlalchemy.types.Integer`,
   :py:obj:`NOT NULL <sqlalchemy.schema.Column.params.nullable>`,
   :py:class:`FK <sqlalchemy.schema.ForeignKeyConstraint>`
   :sql:column:`file.file_id`)

.. sql:column:: release_num

   (:py:class:`~sqlalchemy.types.String`,
   :py:obj:`NOT NULL <sqlalchemy.schema.Column.params.nullable>`)

.. sql:table:: satellite

.. sql:column:: satellite_id

   (:py:class:`~sqlalchemy.types.Integer`,
   :py:class:`PK <sqlalchemy.schema.PrimaryKeyConstraint>`,
   :py:obj:`NOT NULL <sqlalchemy.schema.Column.params.nullable>`)

.. sql:column:: satellite_name

   (:py:class:`~sqlalchemy.types.String`,
   :py:obj:`NOT NULL <sqlalchemy.schema.Column.params.nullable>`)

.. sql:column:: mission_id

   (:py:class:`~sqlalchemy.types.Integer`,
   :py:obj:`NOT NULL <sqlalchemy.schema.Column.params.nullable>`,
   :py:class:`FK <sqlalchemy.schema.ForeignKeyConstraint>`
   :sql:column:`mission.mission_id`)

.. sql:table:: unixtime

.. sql:column:: file_id

   (:py:class:`~sqlalchemy.types.Integer`,
   :py:class:`PK <sqlalchemy.schema.PrimaryKeyConstraint>`,
   :py:obj:`NOT NULL <sqlalchemy.schema.Column.params.nullable>`,
   :py:class:`FK <sqlalchemy.schema.ForeignKeyConstraint>`
   :sql:column:`file.file_id`)

.. sql:column:: unix_start

   (:py:class:`~sqlalchemy.types.Integer`)

.. sql:column:: unix_stop

   (:py:class:`~sqlalchemy.types.Integer`)
