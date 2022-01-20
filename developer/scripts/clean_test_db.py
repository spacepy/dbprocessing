#!/usr/bin/env python

"""Remove records from RBSP_mageis.sqlite to minimize database size.

Reduces size of existing RBSP_mageis database so that it can more
easily be used as unit test data without having to carry the entire
database.
"""

# In order for the new database to exactly match what is created
# from the dump, also need to do:
# ALTER TABLE mission ADD column codedir VARCHAR(50);
# ALTER TABLE mission ADD column inspectordir VARCHAR(50);
# ALTER TABLE mission ADD column errordir VARCHAR(50);

import datetime
import dbprocessing.DButils


def find_related_products(dbu, prod_ids, outputs=False):
    """Find all input/output products for a list of products

    Parameters
    ----------
    dbu : DButils
        Open database connection
    prod_ids : sequence
        Product IDs to search for
    outputs : boolean, default False
        Include all output products as well, not just inputs.

    Returns
    -------
    sequence
        All provided product IDs, plus all products that are inputs for
        them or can be made from them, recursively (i.e. the entire tree).
    """
    to_process = set(prod_ids)
    processed = set()
    while len(to_process):
        in_processes = [dbu.getProcessFromOutputProduct(output_id)
                        for output_id in to_process]
        in_processes = [i for i in in_processes if i is not None]
        in_products = [
            input_id
            for process_id in in_processes
            for input_id, _ in dbu.getInputProductID(process_id)]
        new_products = set(in_products)
        if outputs:
            out_products = [
                output_id for input_id in to_process
                for output_id in dbu.getChildTree(input_id)]
            new_products.update(out_products)
        processed.update(to_process)
        to_process = new_products.difference(processed)
    return sorted(processed)
        

dbu = dbprocessing.DButils.DButils('./RBSP_MAGEIS.sqlite')
# These products are actually used in the unit testing
#keep_products = (1, 2, 3, 4, 5, 8, 10, 13, 60, 119, 138, 181, 187, 190,)
#keep_products = find_related_products(dbu, keep_products)
# This is the expanded set of products to ensure sufficient inputs
# to keep the chain consistent--originally was using find_related_products,
# but in the end used this set.
keep_products = (1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 15, 16, 17, 20, 21, 22, 23, 24, 25, 26, 28, 29, 32, 37, 38, 42, 43, 45, 47, 48, 49, 50, 55, 57, 60, 61, 64, 66, 67, 72, 78, 83, 84, 86, 87, 89, 90, 91, 93, 119, 138, 181, 187, 190)
for prod_id in range(1, 190):
    if prod_id in keep_products:
        continue

    files = [rec.file_id for rec in dbu.getFiles(product=prod_id)]
    for file_id in files:
        dbu._purgeFileFromDB(file_id, trust_id=True, commit=False)

    sq = dbu.session.query(dbu.Instrumentproductlink)\
        .filter_by(product_id=prod_id)
    for ll in list(sq):
        dbu.session.delete(ll)

    sq = dbu.session.query(dbu.Productprocesslink)\
        .filter_by(input_product_id=prod_id)
    results = list(sq)
    for ll in results:
        dbu.session.delete(ll)

    sq = dbu.session.query(dbu.Process).filter_by(output_product=prod_id)
    results = list(sq)
    for ll in results:
        sq = dbu.session.query(dbu.Code).filter_by(process_id=ll.process_id)
        for code in list(sq):
            dbu.session.delete(code)
        sq = dbu.session.query(dbu.Productprocesslink)\
            .filter_by(process_id=ll.process_id)
        for ppl in list(sq):
            dbu.session.delete(ppl)
        dbu.session.delete(ll)

    sq = dbu.session.query(dbu.Inspector).filter_by(product=prod_id)
    results = list(sq)
    for ll in results:
        dbu.session.delete(ll)

    dbu.delProduct(prod_id)  # performs commit

# Only keep a few dates
delme = set()
to_process = set(dbu.getAllFileIds(startDate=datetime.datetime(2013, 9, 11)))
while to_process:
    new_to_process = [rec.resulting_file
                      for file_id in to_process
                      for rec in dbu.session.query(dbu.Filefilelink)
                      .filter_by(source_file=file_id)]
    new_to_process = set(new_to_process)
    delme.update(to_process)
    to_process = new_to_process.difference(delme)
for file_id in delme:
    dbu._purgeFileFromDB(file_id, trust_id=True, commit=False)
dbu.commitDB()
dbu.session.execute('VACUUM')
dbu.commitDB()
