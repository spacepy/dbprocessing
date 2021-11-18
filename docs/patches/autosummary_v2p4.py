"""Patches to extend autosummary for Sphinx 2.4

Sphinx autosummary doesn't get all attributes, and it doesn't include
data members. These patches address those limitations.
"""

# Monkey-patch to put data items in automodule
# This STARTS verbatim from 2.4.4, then new code is commented with ##
# From sphinx/ext/autosummary/generate.py,
# Copyright 2007-2020 by the Sphinx team, BSD license
# 2.3 is identical to 2.4. 2.2 is handled with conditionals.
# 3.0 identical to 2.4 except wording of one warning.
from typing import Any, Callable, Dict, List, Set, Tuple

import sphinx.errors  ## NEW
from sphinx.ext.autosummary import import_by_name, get_documenter
from sphinx.pycode import ModuleAnalyzer  ## NEW
from sphinx.locale import __
from sphinx.util.inspect import safe_getattr
from sphinx import version_info  ## NEW
## NEW: these imports bring in definitions that are available in original
from sphinx.ext.autosummary.generate import AutosummaryRenderer, logger


def generate_autosummary_content(name: str, obj: Any, parent: Any,
                                 template: AutosummaryRenderer, template_name: str,
                                 imported_members: bool, app: Any) -> str:
    doc = get_documenter(app, obj, parent)

    if template_name is None:
        template_name = 'autosummary/%s.rst' % doc.objtype
        if not template.exists(template_name):
            template_name = 'autosummary/base.rst'

    def skip_member(obj: Any, name: str, objtype: str) -> bool:
        try:
            return app.emit_firstresult('autodoc-skip-member', objtype, name,
                                        obj, False, {})
        except Exception as exc:
            logger.warning(__('autosummary: failed to determine %r to be documented.'
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
                    # NEW: conditional and handling from 2.2.2
                    if version_info[:2] == (2, 2):
                        items.append(name)
                        continue
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
        if version_info[:2] == (2, 2):  ## NEW: 2.2's handling, conditional
            public = [x for x in items
                      if x in include_public or not x.startswith('_')]
        return public, items

    ns = {}  # type: Dict[str, Any]

    if doc.objtype == 'module':
        ns['members'] = dir(obj)
        ns['functions'], ns['all_functions'] = \
            get_members(obj, {'function'}, imported=imported_members)
        ns['classes'], ns['all_classes'] = \
            get_members(obj, {'class'}, imported=imported_members)
        ns['exceptions'], ns['all_exceptions'] = \
            get_members(obj, {'exception'}, imported=imported_members)
        ## NEW
        ns['data'], ns['all_data'] = \
            get_members(obj, {'data'}, imported=imported_members)
        ns['attributes'], ns['all_attributes'] = \
            get_members(obj, {'attribute'}, imported=imported_members)
        # Get module-level data attributes
        realmodule = getattr(obj, '__name__', None)
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
                             and k[0] == '']  # module-level only
                ns['all_attributes'].extend(moreattrs)
                ns['attributes'].extend(
                    [a for a in moreattrs if not a.startswith('_')])
        ## END NEW
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

    parts = name.split('.')
    if doc.objtype in ('method', 'attribute', 'property'):
        mod_name = '.'.join(parts[:-2])
        cls_name = parts[-2]
        obj_name = '.'.join(parts[-2:])
        ns['class'] = cls_name
    else:
        mod_name, obj_name = '.'.join(parts[:-1]), parts[-1]

    ns['fullname'] = name
    ns['module'] = mod_name
    ns['objname'] = obj_name
    ns['name'] = parts[-1]

    ns['objtype'] = doc.objtype
    ns['underline'] = len(name) * '='

    return template.render(template_name, ns)
