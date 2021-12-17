"""Patches to extend autosummary for Sphinx 3.1, 3.2

Sphinx autosummary doesn't get all attributes, and it doesn't include
data members. These patches address those limitations.
"""

# Monkey-patch to put data items in automodule
# This STARTS verbatim from 3.1.2, then new code is commented with ##
# 3.2.1, 3.3.1, 3.4.3 identical to 3.1.2
# 3.5.4 has one conditional
# From sphinx/ext/autosummary/generate.py,
# Copyright 2007-2020 by the Sphinx team, BSD license

from typing import Any, Dict, List, Set, Tuple

import sphinx.errors  ## NEW
from sphinx.ext.autosummary import get_documenter
from sphinx.pycode import ModuleAnalyzer  ## NEW
from sphinx.locale import __
from sphinx.util.inspect import safe_getattr
from sphinx import version_info  ## NEW
## NEW: these imports bring in definitions that are available in original
from sphinx.ext.autosummary.generate import AutosummaryRenderer, logger, ModuleScanner
def generate_autosummary_content(name: str, obj: Any, parent: Any,
                                 template: AutosummaryRenderer, template_name: str,
                                 imported_members: bool, app: Any,
                                 recursive: bool, context: Dict,
                                 modname: str = None, qualname: str = None,
                                 encoding = None) -> str:
    doc = get_documenter(app, obj, parent)

    def skip_member(obj: Any, name: str, objtype: str) -> bool:
        try:
            return app.emit_firstresult('autodoc-skip-member', objtype, name,
                                        obj, False, {})
        except Exception as exc:
            logger.warning(__('autosummary: failed to determine %r to be documented, '
                              'the following exception was raised:\n%s'),
                           name, exc, type='autosummary')
            return False

    def get_members(obj: Any, types: Set[str], include_public: List[str] = [],
                    imported: bool = True) -> Tuple[List[str], List[str]]:
        items = []  # type: List[str]
        public = []  # type: List[str]
        for name in dir(obj):
            try:
                value = safe_getattr(obj, name)
            except AttributeError:
                continue
            documenter = get_documenter(app, value, obj)
            if documenter.objtype in types:
                # skip imported members if expected
                if imported or getattr(value, '__module__', None) == obj.__name__:
                    skipped = skip_member(value, name, documenter.objtype)
                    if skipped is True:
                        pass
                    elif skipped is False:
                        # show the member forcedly
                        items.append(name)
                        public.append(name)
                    else:
                        items.append(name)
                        if name in include_public or not name.startswith('_'):
                            # considers member as public
                            public.append(name)
        return public, items

    def get_module_attrs(members: Any) -> Tuple[List[str], List[str]]:
        """Find module attributes with docstrings."""
        attrs, public = [], []
        try:
            analyzer = ModuleAnalyzer.for_module(name)
            attr_docs = analyzer.find_attr_docs()
            for namespace, attr_name in attr_docs:
                if namespace == '' and attr_name in members:
                    attrs.append(attr_name)
                    if not attr_name.startswith('_'):
                        public.append(attr_name)
        except PycodeError:
            pass    # give up if ModuleAnalyzer fails to parse code
        return public, attrs

    def get_modules(obj: Any) -> Tuple[List[str], List[str]]:
        items = []  # type: List[str]
        for _, modname, ispkg in pkgutil.iter_modules(obj.__path__):
            fullname = name + '.' + modname
            ## NEW from 3.5, plus conditional
            if version_info[:2] == (3, 5):
                try:
                    module = import_module(fullname)
                    if module and hasattr(module, '__sphinx_mock__'):
                        continue
                except ImportError:
                    pass
            ## END NEW
            items.append(fullname)
        public = [x for x in items if not x.split('.')[-1].startswith('_')]
        return public, items

    ns = {}  # type: Dict[str, Any]
    ns.update(context)

    if doc.objtype == 'module':
        scanner = ModuleScanner(app, obj)
        ns['members'] = scanner.scan(imported_members)
        ns['functions'], ns['all_functions'] = \
            get_members(obj, {'function'}, imported=imported_members)
        ns['classes'], ns['all_classes'] = \
            get_members(obj, {'class'}, imported=imported_members)
        ns['exceptions'], ns['all_exceptions'] = \
            get_members(obj, {'exception'}, imported=imported_members)
        ns['attributes'], ns['all_attributes'] = \
            get_module_attrs(ns['members'])
        ispackage = hasattr(obj, '__path__')
        if ispackage and recursive:
            ns['modules'], ns['all_modules'] = get_modules(obj)
    elif doc.objtype == 'class':
        ns['members'] = dir(obj)
        ns['inherited_members'] = \
            set(dir(obj)) - set(obj.__dict__.keys())
        ns['methods'], ns['all_methods'] = \
            get_members(obj, {'method'}, ['__init__'])
        ns['attributes'], ns['all_attributes'] = \
            get_members(obj, {'attribute', 'property'})
        ## NEW
        # Try to get stuff that's only in class attributes
        if hasattr(obj, '__module__'):
            realmodule = obj.__module__
        elif hasattr(parent, '__module__'):
            realmodule = parent.__module__
        else:
            realmodule = None
        if realmodule:
            #Keyed by a tuple of (class name, attribute name)
            #Result is a list of docstrings
            try:
                docattrs = ModuleAnalyzer.for_module(
                    realmodule).find_attr_docs()
            except sphinx.errors.PycodeError:
                pass  # No source code (e.g. internal module)
            else:
                moreattrs = [k[1] for k in docattrs.keys()
                             if k[1] not in ns['all_attributes']
                             and k[0] == obj.__name__]
                ns['all_attributes'].extend(moreattrs)
                ns['attributes'].extend(
                    [a for a in moreattrs if not a.startswith('_')])
        ## END NEW

    if modname is None or qualname is None:
        modname, qualname = split_full_qualified_name(name)

    if doc.objtype in ('method', 'attribute', 'property'):
        ns['class'] = qualname.rsplit(".", 1)[0]

    if doc.objtype in ('class',):
        shortname = qualname
    else:
        shortname = qualname.rsplit(".", 1)[-1]

    ns['fullname'] = name
    ns['module'] = modname
    ns['objname'] = qualname
    ns['name'] = shortname

    ns['objtype'] = doc.objtype
    ns['underline'] = len(name) * '='

    if template_name:
        return template.render(template_name, ns)
    else:
        return template.render(doc.objtype, ns)
