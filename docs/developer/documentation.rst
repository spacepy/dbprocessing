#############
Documentation
#############

`Sphinx <http://www.sphinx-doc.org/>`_ documentation is stored in the ``sphinx`` directory.

``Makefile`` is used to build the documentation.

Run ``make html`` in the ``sphinx`` directory to build the html documentation, output is in ``build/html``.

``build`` contains the built documentation (and intermediate files); it can safely be deleted.

``source`` contains the ReStructuredText source files; note that a large quantity of the documentation is not built from here but from the Python source files. ``source/autosummary`` contains the docs extracted from those source files; it can safely be deleted. If anything's weird about the docs generated from the Python source, try deleting ``autosummary`` first and then rebuilding. The ``autosummary`` docs are extracted from the version of the module in top-level ``build``, i.e., run ``python setup.py build`` before generating the documentation.
