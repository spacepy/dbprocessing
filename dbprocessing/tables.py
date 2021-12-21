#!/usr/bin/env python
"""Table definitions for dbprocessing.

Contains definitions for all database tables, keyed by table name and
suitable for passing as arguments to the :class:`~sqlalchemy.schema.Table`
constructor.
"""

from sqlalchemy import schema, types


names = ['mission', 'satellite', 'instrument', 'product',
         'instrumentproductlink', 'process', 'productprocesslink',
         'file', 'unixtime', 'filefilelink', 'code', 'processqueue',
         'filecodelink', 'release', 'logging', 'logging_file', 'inspector',
         ]
"""Names of tables, in order they should be created (as a table should
be defined before it's linked to), although a table does not necessarily
depend on *all* tables coming before it in this list."""

# NOTE: if one stops using sqlite then change file_id, logging_id and file_logging_id
#       to BigIntegers (sqlite doesn't know BigInteger)
# TODO this can/should all be redone using the new syntax and relations
# see: http://docs.sqlalchemy.org/en/rel_0_7/orm/relationships.html# for
# some examples.
# TODO: It would also be useful to be able to look up a table name and get
# all the tables it requires.
# This is a function rather than dict so that separate column objects are
# created on each call; otherwise reusing the object if e.g. opening two
# databases without quitting Python causes problems (since it gets
# associated with a particular database).
def definition(name):
    """Get definition of a table

    Parameters
    ----------
    name : :class:`str`
        Name of the table to get definition for, see :data:`names`.

    Returns
    -------
    :class:`tuple`
        Arguments for :class:`~sqlalchemy.schema.Table` constructor, such that
        it can be called
        ``Table(name, metadata, *dbprocessing.tables.definition[name])``

    Raises
    ------
    ValueError
        if the name is not recognized
    """
    if name == 'mission':
        return (
            schema.Column('mission_id', types.Integer, autoincrement=True, primary_key=True,
                          nullable=False),
            schema.Column('mission_name', types.String(20), nullable=False, unique=True),
            schema.Column('rootdir', types.String(250), nullable=False, ),
            schema.Column('incoming_dir', types.String(250), nullable=False, ),
            schema.Column('codedir', types.String(250), nullable=True, ),
            schema.Column('inspectordir', types.String(250), nullable=True, ),
            schema.Column('errordir', types.String(250), nullable=True, )
        )
    elif name == 'satellite':
        return (
            schema.Column('satellite_id', types.Integer, autoincrement=True, primary_key=True,
                          nullable=False),
            schema.Column('satellite_name', types.String(20), nullable=False),  # hmm long enough?
            schema.Column('mission_id', types.Integer,
                          schema.ForeignKey('mission.mission_id'), nullable=False, ),
            schema.UniqueConstraint('satellite_name', 'mission_id', name='unique_pairs_satellite')
        )
    elif name == 'instrument':
        return (
            schema.Column('instrument_id', types.Integer, autoincrement=True, primary_key=True,
                          nullable=False),
            schema.Column('instrument_name', types.String(20), nullable=False),
            # hmm long enough?
            schema.Column('satellite_id', types.Integer,
                          schema.ForeignKey('satellite.satellite_id'), nullable=False, ),
            schema.UniqueConstraint('instrument_name', 'satellite_id',
                                    name='unique_pairs_instrument')
        )
    elif name == 'product':
        return (
            schema.Column('product_id', types.Integer, autoincrement=True, primary_key=True,
                          nullable=False, index=True),
            schema.Column('product_name', types.String(100), nullable=False, index=True),
            # hmm long enough?
            schema.Column('instrument_id', types.Integer,
                          schema.ForeignKey('instrument.instrument_id'), nullable=False, ),
            schema.Column('relative_path', types.String(250), nullable=False),  # hmm long enough?
            schema.Column('level', types.Float, nullable=False),
            schema.Column('format', types.Text, nullable=False),  # hmm long enough?
            schema.Column('product_description', types.Text, nullable=True),  # hmm long enough?
            schema.UniqueConstraint('product_name', 'instrument_id', 'relative_path',
                                    name='unique_triplet_product')
        )
    elif name == 'instrumentproductlink':
        return (
            schema.Column('instrument_id', types.Integer,
                          schema.ForeignKey('instrument.instrument_id'), nullable=False),
            schema.Column('product_id', types.Integer,
                          schema.ForeignKey('product.product_id'), nullable=False),
            schema.PrimaryKeyConstraint('instrument_id', 'product_id')
        )
    elif name == 'process':
        return (
            schema.Column('process_id', types.Integer, autoincrement=True, primary_key=True,
                          nullable=False, index=True),
            schema.Column('process_name', types.String(50), nullable=False),  # hmm long enough?
            schema.Column('output_product', types.Integer,
                          schema.ForeignKey('product.product_id'), nullable=True, index=True),
            schema.Column('output_timebase', types.String(10), nullable=True, index=True),
            schema.Column('extra_params', types.Text, nullable=True),
            schema.UniqueConstraint('process_name', 'output_product')
        )
    elif name == 'productprocesslink':
        return (
            schema.Column('process_id', types.Integer,
                          schema.ForeignKey('process.process_id'), nullable=False),
            schema.Column('input_product_id', types.Integer,
                          schema.ForeignKey('product.product_id'), nullable=False),
            schema.Column('optional', types.Boolean, nullable=False),
            schema.Column('yesterday', types.Integer, nullable=False),
            schema.Column('tomorrow', types.Integer, nullable=False),
            schema.PrimaryKeyConstraint('process_id', 'input_product_id')
        )
    elif name == 'file':
        return (
            # this was a bigint, sqlalchemy doesn't seem to like this... think here
            schema.Column('file_id', types.Integer, autoincrement=True, primary_key=True,
                          nullable=False, index=True),
            schema.Column('filename', types.String(250), nullable=False, unique=True, index=True),
            schema.Column('utc_file_date', types.Date, nullable=True, index=True),
            schema.Column('utc_start_time', types.DateTime, nullable=True, index=True),
            schema.Column('utc_stop_time', types.DateTime, nullable=True, index=True),
            schema.Column('data_level', types.Float, nullable=False, index=True),
            schema.Column('interface_version', types.SmallInteger, nullable=False),
            schema.Column('quality_version', types.SmallInteger, nullable=False),
            schema.Column('revision_version', types.SmallInteger, nullable=False),
            schema.Column('verbose_provenance', types.Text, nullable=True),
            schema.Column('check_date', types.DateTime, nullable=True),
            schema.Column('quality_comment', types.Text, nullable=True),
            schema.Column('caveats', types.Text, nullable=True),
            schema.Column('file_create_date', types.DateTime, nullable=False),
            schema.Column('met_start_time', types.Float, nullable=True),
            schema.Column('met_stop_time', types.Float, nullable=True),
            schema.Column('exists_on_disk', types.Boolean, nullable=False),
            schema.Column('quality_checked', types.Boolean, nullable=True, default=False),
            schema.Column('product_id', types.Integer,
                          schema.ForeignKey('product.product_id'), nullable=False),
            schema.Column('shasum', types.String(40), nullable=True),
            schema.Column('process_keywords', types.Text, nullable=True),
            schema.CheckConstraint('utc_stop_time is not NULL OR met_stop_time is not NULL'),
            schema.CheckConstraint('utc_start_time is not NULL OR met_start_time is not NULL'),
            schema.CheckConstraint('met_start_time <= met_stop_time'),  # in case of one entry
            schema.CheckConstraint('utc_start_time <= utc_stop_time'),  # in case of one entry
            schema.CheckConstraint('interface_version >= 1'),
            schema.UniqueConstraint('utc_file_date',
                                    'product_id',
                                    'interface_version',
                                    'quality_comment',
                                    'revision_version', name='Unique file tuple'),
            schema.Index('ix_file_big', 'filename', 'utc_file_date',
                         'utc_start_time', 'utc_stop_time', unique=True),
        )
    elif name == 'unixtime':
        return (
            schema.Column('file_id', types.Integer,
                          schema.ForeignKey('file.file_id'), primary_key=True, index=True),
            schema.Column('unix_start', types.Integer, index=True),
            schema.Column('unix_stop', types.Integer, index=True),
            schema.CheckConstraint('unix_start <= unix_stop'),
        )
    elif name == 'filefilelink':
        return (
            schema.Column('source_file', types.Integer,
                          schema.ForeignKey('file.file_id'), nullable=False, index=True),
            schema.Column('resulting_file', types.Integer,
                          schema.ForeignKey('file.file_id'), nullable=False, index=True),
            schema.PrimaryKeyConstraint('source_file', 'resulting_file'),
            schema.CheckConstraint('source_file <> resulting_file'),
            # TODO this is supposed to be more general than !=
        )
    elif name == 'code':
        return (
            schema.Column('code_id', types.Integer, autoincrement=True, primary_key=True,
                          nullable=False, index=True),
            schema.Column('filename', types.String(250), nullable=False, unique=False),
            schema.Column('relative_path', types.String(250), nullable=False),
            schema.Column('code_start_date', types.Date, nullable=False),
            schema.Column('code_stop_date', types.Date, nullable=False),
            schema.Column('code_description', types.Text, nullable=False),
            schema.Column('process_id', types.Integer,
                          schema.ForeignKey('process.process_id'), nullable=False, index=True),
            schema.Column('interface_version', types.SmallInteger, nullable=False),
            schema.Column('quality_version', types.SmallInteger, nullable=False),
            schema.Column('revision_version', types.SmallInteger, nullable=False),
            schema.Column('output_interface_version', types.SmallInteger, nullable=False),
            schema.Column('active_code', types.Boolean, nullable=False, default=False),
            schema.Column('date_written', types.Date, nullable=False),
            schema.Column('shasum', types.String(40), nullable=True),
            schema.Column('newest_version', types.Boolean, nullable=False),
            schema.Column('arguments', types.Text, nullable=True),
            schema.Column('ram', types.Float, nullable=True),  # amanount of ram used in Gigs
            schema.Column('cpu', types.SmallInteger, nullable=True),  # number of cpus used
            schema.CheckConstraint('code_start_date <= code_stop_date'),
            schema.CheckConstraint('interface_version >= 1'),
            schema.CheckConstraint('output_interface_version >= 1'),
        )
    elif name == 'processqueue':
        return (
            schema.Column('file_id', types.Integer,
                          schema.ForeignKey('file.file_id'),
                          primary_key=True, nullable=False, unique=True, index=True),
            schema.Column('version_bump', types.SmallInteger, nullable=True),
            schema.CheckConstraint('version_bump is NULL or version_bump < 3'),
        )
    elif name == 'filecodelink':
        return (
            schema.Column('resulting_file', types.Integer,
                          schema.ForeignKey('file.file_id'), nullable=False),
            schema.Column('source_code', types.Integer,
                          schema.ForeignKey('code.code_id'), nullable=False),
            schema.PrimaryKeyConstraint('resulting_file', 'source_code')
        )
    elif name == 'release':
        return (
            schema.Column('file_id', types.Integer,
                          schema.ForeignKey('file.file_id'), nullable=False, ),
            schema.Column('release_num', types.String(20), nullable=False),
            schema.PrimaryKeyConstraint('file_id', 'release_num')
        )
    elif name == 'logging':
        return (
            schema.Column('logging_id', types.Integer, autoincrement=True, primary_key=True,
                          nullable=False),
            schema.Column('currently_processing', types.Boolean, nullable=False, default=False),
            schema.Column('pid', types.Integer, nullable=True),
            schema.Column('processing_start_time', types.DateTime, nullable=False),
            # might have to be a TIMESTAMP
            schema.Column('processing_end_time', types.DateTime, nullable=True),
            schema.Column('comment', types.Text, nullable=True),
            schema.Column('mission_id', types.Integer,
                          schema.ForeignKey('mission.mission_id'), nullable=False),
            schema.Column('user', types.String(30), nullable=False),
            schema.Column('hostname', types.String(100), nullable=False),
            # schema.PrimaryKeyConstraint('logging_id'),
            schema.CheckConstraint('processing_start_time < processing_end_time'),
        )
    elif name == 'logging_file':
        return (
            schema.Column('logging_file_id', types.Integer, autoincrement=True, primary_key=True,
                          nullable=False),
            schema.Column('logging_id', types.Integer,
                          schema.ForeignKey('logging.logging_id'), nullable=False),
            schema.Column('file_id', types.Integer,
                          schema.ForeignKey('file.file_id'), nullable=False),
            schema.Column('code_id', types.Integer,
                          schema.ForeignKey('code.code_id'), nullable=False),
            schema.Column('comments', types.Text, nullable=True),
            # schema.PrimaryKeyConstraint('logging_file_id'),
        )
    elif name == 'inspector':
        return (
            schema.Column('inspector_id', types.Integer, autoincrement=True, primary_key=True,
                          nullable=False, index=True),
            schema.Column('filename', types.String(250), nullable=False, unique=False),
            schema.Column('relative_path', types.String(250), nullable=False),
            schema.Column('description', types.Text, nullable=False),
            schema.Column('interface_version', types.SmallInteger, nullable=False),
            schema.Column('quality_version', types.SmallInteger, nullable=False),
            schema.Column('revision_version', types.SmallInteger, nullable=False),
            schema.Column('output_interface_version', types.SmallInteger, nullable=False),
            schema.Column('active_code', types.Boolean, nullable=False, default=False,
                          index=True),
            schema.Column('date_written', types.Date, nullable=False),
            schema.Column('shasum', types.String(40), nullable=True),
            schema.Column('newest_version', types.Boolean, nullable=False, index=True),
            schema.Column('arguments', types.Text, nullable=True),
            schema.Column('product', types.Integer,
                          schema.ForeignKey('product.product_id'), nullable=False),
            schema.CheckConstraint('interface_version >= 1'),
            schema.CheckConstraint('output_interface_version >= 1'),
        )
    else:
        raise ValueError('Unknown table {}'.format(name))
