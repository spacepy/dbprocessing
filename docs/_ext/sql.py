import docutils.parsers.rst
import sphinx
import sphinx.addnodes
import sphinx.directives
import sphinx.domains
import sphinx.locale
import sphinx.roles
import sphinx.util
import sphinx.util.nodes


class ColumnDirective(sphinx.directives.ObjectDescription):
    """Description of a database column (.. column)."""

    has_content = True
    required_arguments = 1  # Name of the item

    def handle_signature(self, sig, signode):
        signode.clear()
        # Alternatives: desc_addname (monospace), desc_annotation (italic)
        signode += sphinx.addnodes.desc_type(text=sig)
        # Remove whitespace in target
        name = sphinx.util.ws_re.sub('', sig)
        return name

    def add_target_and_index(self, name, sig, signode):
        table_ws = self.env.ref_context.get('sql:table')
        table = sphinx.util.ws_re.sub('', table_ws) if table_ws else None
        targetname = '%s-%s-%s' % (self.objtype, table, name) if table\
                     else '%s-%s' % (self.objtype, name)
        signode['ids'].append(targetname)
        self.state.document.note_explicit_target(signode)
        self.indexnode['entries'].append((
            'single',
            '{}; ({}column)'.format(sig, table_ws + ' ' if table_ws else ''),
            targetname, '', None))
        qualname = '{}.{}'.format(table, name) if table else name
        self.env.domaindata['sql']['objects'][self.objtype, qualname] = \
            self.env.docname, targetname


class ColumnXRefRole(sphinx.roles.XRefRole):
    def process_link(self, env, refnode, has_explicit_title, title, target):
        table = env.ref_context.get('sql:table')
        if table:
            sphinx.util.ws_re.sub('', table)
        # TODO need better way of distinguishing between qualified and not
        if table and '.' not in target:
            target = '{}.{}'.format(table, target)
        if target.startswith('~') and '.' in title and not has_explicit_title:
            target = target[1:]
            title = title.split('.', 1)[1]
        return title, sphinx.util.ws_re.sub('', target)


class CurrentTableDirective(docutils.parsers.rst.Directive):
    """Change the current table, no output"""

    has_content = False
    required_arguments = 0
    optional_arguments = 1
    final_argument_whitespace = True
    option_spec = {}  # type: Dict

    def run(self):
        env = self.state.document.settings.env
        if self.arguments:
            self.state.document.settings.env.ref_context['sql:table']\
                = self.arguments[0]
        else:
            self.state.document.settings.env.ref_context.pop(
                'sql:table', None)
        return []


class TableDirective(sphinx.directives.ObjectDescription):
    """Description of a database table (.. table)."""

    has_content = True
    required_arguments = 1  # Name of the item

    def handle_signature(self, sig, signode):
        signode.clear()
        signode += sphinx.addnodes.desc_name(text=sig)
        name = sphinx.util.ws_re.sub('', sig)
        # Store table name w/any whitespace, so human-readable
        self.state.document.settings.env.ref_context['sql:table'] = sig
        return name

    def add_target_and_index(self, name, sig, signode):
        targetname = '%s-%s' % (self.objtype, name)
        signode['ids'].append(targetname)
        self.state.document.note_explicit_target(signode)
        self.indexnode['entries'].append((
            'single', '{}; (table)'.format(name), targetname, '', None))
        self.env.domaindata['sql']['objects'][self.objtype, name] = \
            self.env.docname, targetname


class TableXRefRole(sphinx.roles.XRefRole):
    def process_link(self, env, refnode, has_explicit_title, title, target):
        return title, sphinx.util.ws_re.sub('', target)


class SqlDomain(sphinx.domains.Domain):

    name = 'sql'
    label = 'SQL'
    directives = {
        'column': ColumnDirective,
        'currenttable': CurrentTableDirective,
        'table': TableDirective,
    }
    roles = {
        'column': ColumnXRefRole(warn_dangling=True),
        'table': TableXRefRole(warn_dangling=True),
    }
    object_types = {
        'column': sphinx.domains.ObjType('column', 'column'),
        'table': sphinx.domains.ObjType('table', 'table'),
    }

    initial_data = {
        'objects': {},
    }

    def resolve_xref(self, env, fromdocname, builder, typ, target,
                     node, contnode):
        # Cribbed straight from the standard domain
        objtypes = self.objtypes_for_role(typ) or []
        for objtype in objtypes:
            if (objtype, target) in self.data['objects']:
                docname, labelid = self.data['objects'][objtype, target]
                break
        else:
            docname, labelid = '', ''
        if not docname:
            return None
        return sphinx.util.nodes.make_refnode(builder, fromdocname, docname,
                                              labelid, contnode)

    def merge_domaindata(self, docnames, otherdata):
        for key, data in otherdata['objects'].items():
            if data[0] in docnames:
                self.data['objects'] = data
        super(SqlDomain, self).merge_domaindata(docnames, otherdata)

def setup(app):
    app.add_domain(SqlDomain)
    return {
        'version': '0.1',
        'parallel_read_safe': True,
        }
