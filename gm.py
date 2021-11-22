#!/usr/bin/python
# -*- coding: utf-8 -*-
import copy
import os
import json

from loguru import logger

from Qt import QtCore, QtGui, QtWidgets

from NodeGraphQt import (NodeGraph,
                         BaseNode,
                         BackdropNode,
                         PropertiesBinWidget,
                         setup_context_menu, NodeGraphMenu)

# import example nodes from the "example_nodes" package
from NodeGraphQt.widgets.actions import BaseMenu
from NodeGraphQt.widgets.dialogs import FileDialog
from example_nodes import basic_nodes, widget_nodes


from mdls import parse_all


def main():
    QtCore.QCoreApplication.setAttribute(QtCore.Qt.AA_EnableHighDpiScaling)
    app = QtWidgets.QApplication([])

    # create node graph.
    graph = NodeGraph()

    # set up default menu and commands.
    setup_context_menu(graph)

    # widget used for the node graph.
    graph_widget = graph.widget
    graph_widget.resize(1200, 900)
    graph_widget.show()


    # show the properties bin when a node is "double clicked" in the graph.
    properties_bin = PropertiesBinWidget(node_graph=graph)
    # setWindowFlags(Qt.CustomizeWindowHint | Qt.FramelessWindowHint | Qt.Dialog | Qt.WindowStaysOnTopHint | Qt.Tool)
    properties_bin.setWindowFlags(QtCore.Qt.Tool | QtCore.Qt.Dialog | QtCore.Qt.WindowStaysOnTopHint)
    def show_prop_bin(node):
        if not properties_bin.isVisible():
            properties_bin.show()
            # print(type(properties_bin), dir(properties_bin))
    graph.node_double_clicked.connect(show_prop_bin)

    nodes_to_reg = []
    mods = parse_all()
    for m in mods:
        # https://www.tutorialdocs.com/article/python-class-dynamically.html
        cls = type(m.name, (BaseNode, object,),
                   {'__identifier__': m.catalog, 'NODE_NAME': m.desc,
                    '__init__': my_init(m) } )
        nodes_to_reg.append(cls)

    graph.register_nodes(nodes_to_reg)

    # auto layout nodes.
    # graph.auto_layout_nodes()

    root_menu = graph.get_context_menu('graph')

    file_menu = root_menu.get_menu('&File')
    file_menu.add_command('Export', exporter(mods), 'Ctrl+E')

    # saveAction = QAction("Save", self)
    # saveAction.setShortcut('Ctrl+S')
    # saveAction.triggered.connect(self.nodegraph.bt_savefile)
    # fileMenu.addAction(saveAction)

    # # wrap a backdrop node.
    # backdrop_node = graph.create_node('nodeGraphQt.nodes.BackdropNode')
    # backdrop_node.wrap_nodes([text_node, checkbox_node])

    # graph.fit_to_selection()

    app.exec_()

def exporter(mods):
    _file = []
    current_dir = [None,]
    def _save_func(graph):
        if len(_file) == 0:
            ext_map = {'GUI Train Flow File(*json)': '.json',
                       'All Files (*)': ''}
            file_dlg = FileDialog.getSaveFileName(
                graph.widget, 'Export GUI Train Flow', current_dir[0], ';;'.join(ext_map.keys()))
            file_path = file_dlg[0]
            if not file_path:
                return
            current_dir[0] = os.path.dirname(file_path)
            logger.info("export to file...{}", file_path)
            _file.append(file_path)
        else:
            logger.info("export to exists file {}", _file[0])
        file_path = _file[0]

        nodes = graph.all_nodes()
        serialized_data = graph._serialize(nodes)
        mods_ = { m.type_:m for m in mods }
        nodes = []

        for id, n in serialized_data['nodes'].items():
            d = mods_[n['type_']]
            m = {"id":id, "name": f"{d['pkg']}.{d['name']}"}
            nodes.append(m)
            custom_param = n.get('custom', {})
            inparams = []
            outparams = []
            for pi in d.inparams:
                pname = pi['name']
                val = custom_param.get(pname, '')
                if val != '':
                    pi['value'] = custom_param[pname]
                elif 'defval' in pi and pi.get('type', 'val') == 'val':
                    pi['value'] = pi['defval']
                for cc in serialized_data['connections']:
                    # {"out": ["0x2907ba2b0a0", "dataframe"], "in": ["0x2907ba2b730", "df"]}
                    if cc['in'][0] == id and cc['in'][1] == pname:
                        pi['from'] = {'node':cc['out'][0], 'param':cc['out'][1]}
                if 'linkable' in pi: pi.pop('linkable')
                if 'desc' in pi: pi.pop('desc')
                if 'defval' in pi: pi.pop('defval')
                inparams.append(pi)
            m['inparams'] = inparams
            for pi in d.outparams:
                if 'linkable' in pi: pi.pop('linkable')
                if 'desc' in pi: pi.pop('desc')
                outparams.append(pi)
            m['outparams'] = outparams
        with open(file_path, 'w', encoding='utf8') as fo:
            json.dump({"nodes":nodes, }, fo, indent=2)
    return _save_func

def my_init(m_data):
    def _func(self):
        BaseNode.__init__(self)
        m = copy.deepcopy(m_data)
        [self.add_input(i['name']) for i in m.inparams if i.get('linkable', False)]
        [self.add_output(o['name']) for o in m.outparams if o.get('linkable', False)]

        [self.add_text_input(i['name'], i['name'], tab='widgets') for i in m.inparams if not i.get('linkable', False)]
        self.m = m

        s: BaseNode = self

    return _func

if __name__ == '__main__':
    main()
