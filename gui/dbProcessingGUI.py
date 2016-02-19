
## pyside-uic MainUI.ui -o MainUI.py

import datetime
import sys

from PySide import QtCore, QtGui

from MainUI import Ui_MainWindow

import dbprocessing.DButils as DBUtils


class MyMainWindow(QtGui.QMainWindow):
    def __init__(self, parent=None):
        super(MyMainWindow, self).__init__(parent)

        self.dbu = DBUtils.DButils(sys.argv[1])

        self.ui = Ui_MainWindow()
        self.ui.setupUi(self)
        self.ui.stackedWidget.setVisible(False)

        self.rightSide = {}
        self.rightSide['product'] = []
        self.rightSide['mission'] = []
        self.rightSide['satellite'] = []
        self.rightSide['instrument'] = []
        self.ui.treeWidget.connect(self.ui.treeWidget.selectionModel(),
                     QtCore.SIGNAL("selectionChanged(QItemSelection, QItemSelection)"),
                     self._debugfn)

        self._updateInternals()
#        self._populateExisting()

    def keyPressEvent(self, event):
        """
        deal with key presses
        """
        print event.key(), '5'
        if event.key() in (16777220, QtCore.Qt.Key_Enter): # figure this out!!
            self._fillRightSide(self._getSelected(), self._getSelected())

    def _initialTree(self):
        """
        populate the initial tree with placeholders
        """
        item = QtGui.QTreeWidgetItem(self.ui.treeWidget.topLevelItem(0)) # satellite
        item.setText(0, QtGui.QApplication.translate("MainWindow", "Satellite", None, QtGui.QApplication.UnicodeUTF8))
        item = QtGui.QTreeWidgetItem(self.ui.treeWidget.topLevelItem(0).child(0)) # instrument
        item.setText(0, QtGui.QApplication.translate("MainWindow", "Instrument", None, QtGui.QApplication.UnicodeUTF8))
        item = QtGui.QTreeWidgetItem(self.ui.treeWidget.topLevelItem(0).child(0).child(0)) # product
        item.setText(0, QtGui.QApplication.translate("MainWindow", "Product", None, QtGui.QApplication.UnicodeUTF8))
        item = QtGui.QTreeWidgetItem(self.ui.treeWidget.topLevelItem(0).child(0).child(0).child(0)) # inspector
        item.setText(0, QtGui.QApplication.translate("MainWindow", "Inspector", None, QtGui.QApplication.UnicodeUTF8))
        self.ui.treeWidget.connect(self.ui.treeWidget.selectionModel(),
                     QtCore.SIGNAL("selectionChanged(QItemSelection, QItemSelection)"),
                     self._debugfn)
        self.nameIndexMapping = {}
        for i in range(100): # never be this many
            tmp = self.ui.stackedWidget.widget(i)
            if tmp is not None:
                self.nameIndexMapping[i] = (tmp, tmp.objectName())
            else:
                break

    def _updateInternals(self):
        """
        update some of the internal state, call thi often
        """
        self.nameIndexMapping = {}
        for i in range(100): # never be this many
            tmp = self.ui.stackedWidget.widget(i)
            if tmp is not None:
                self.nameIndexMapping[i] = (tmp, tmp.objectName())
            else:
                break

    def _fillRightSide(self, inItem, extra=None):
        """
        display the right side and get the values
        """
        if not self.ui.stackedWidget.isVisible():
            self.ui.stackedWidget.setVisible(True)
        for v in self.nameIndexMapping:
            if inItem.text(0).lower() == self.nameIndexMapping[v][1]:
                self.ui.stackedWidget.setCurrentIndex(v)
                break
        # call the right method to deal with the v=boxes on the right
        getattr(self, '_' + self.nameIndexMapping[v][1] + 'Right' )(extra) # call the right method

    def _getSelected(self):
        """
        go through the left side tree and return the selected object
        """
        for v in self.nameIndexMapping:
            for tl_idx in range(self.ui.treeWidget.topLevelItemCount()):
               tl_item = self.ui.treeWidget.topLevelItem(tl_idx)
               if tl_item.isSelected():
                   return tl_item
               for ch_idx0 in range(tl_item.childCount()):
                   if tl_item.child(ch_idx0).isSelected():
                       return tl_item.child(ch_idx0)
                   for ch_idx1 in range(tl_item.child(ch_idx0).childCount()):
                       if tl_item.child(ch_idx0).child(ch_idx1).isSelected():
                           return tl_item.child(ch_idx0).child(ch_idx1)
                       for ch_idx2 in range(tl_item.child(ch_idx0).child(ch_idx1).childCount()):
                           if tl_item.child(ch_idx0).child(ch_idx1).child(ch_idx2).isSelected():
                               return tl_item.child(ch_idx0).child(ch_idx1).child(ch_idx2)
                           for ch_idx3 in range(tl_item.child(ch_idx0).child(ch_idx1).child(ch_idx2).childCount()):
                               if tl_item.child(ch_idx0).child(ch_idx1).child(ch_idx2).child(ch_idx3).isSelected():
                                   return tl_item.child(ch_idx0).child(ch_idx1).child(ch_idx2).child(ch_idx3)
                               for ch_idx4 in range(tl_item.child(ch_idx0).child(ch_idx1).child(ch_idx2).child(ch_idx3).childCount()):
                                   if tl_item.child(ch_idx0).child(ch_idx1).child(ch_idx2).child(ch_idx3).child(ch_idx4).isSelected():
                                       return tl_item.child(ch_idx0).child(ch_idx1).child(ch_idx2).child(ch_idx3).child(ch_idx4)




    def _debugfn(self):
        # figure out which is selected
        print self._getSelected().text(0)
        self._fillRightSide(self._getSelected())

    def _newSatellite(self):
        """
        add a satellite to the tree
        """
        item = QtGui.QTreeWidgetItem(self.ui.treeWidget.topLevelItem(self.ui.treeWidget.topLevelItemCount()-1)) # satellite
        item.setText(self.ui.treeWidget.topLevelItemCount()-1, QtGui.QApplication.translate("MainWindow", "Satellite", None, QtGui.QApplication.UnicodeUTF8))
        item.parent().setExpanded(True)

    def _newInstrument(self):
        """
        add a instrument to the tree
        """
        item = QtGui.QTreeWidgetItem(self.ui.treeWidget.topLevelItem(0).child(0)) # instrument
        item.setText(0, QtGui.QApplication.translate("MainWindow", "Instrument", None, QtGui.QApplication.UnicodeUTF8))
        item.parent().setExpanded(True)
        self._updateInternals()

    def _newProduct(self):
        """
        add a product to the tree
        """
        item = QtGui.QTreeWidgetItem(self.ui.treeWidget.topLevelItem(0).child(0).child(0)) # product
        item.setText(0, QtGui.QApplication.translate("MainWindow", "Product", None, QtGui.QApplication.UnicodeUTF8))
        item.parent().setExpanded(True)
        self._updateInternals()

    def _newCode(self):
        """
        add a Code to the tree
        """
        print "_newCode"
        self._updateInternals()

    def _newInspector(self):
        """
        add a inspector to the tree
        """
        item = QtGui.QTreeWidgetItem(self.ui.treeWidget.topLevelItem(0).child(0).child(0).child(0)) # inspector
        item.setText(0, QtGui.QApplication.translate("MainWindow", "Inspector", None, QtGui.QApplication.UnicodeUTF8))
        item.parent().setExpanded(True)
        self._updateInternals()

    def _missionRight(self, extra=None):
        """
        handle things when the mission is on the right
        """
        self.ui.treeWidget.topLevelItem(0).setText(0, self.dbu.mission)
        self.ui.missionNameEntry.setText(self.dbu.mission)
        self.ui.missionDirectoryEntry.setText('mission dir')
        self.ui.newSatelliteButton.connect(self.ui.newSatelliteButton,
                                           QtCore.SIGNAL('clicked()'),
                                           self._newSatellite)

    def _satelliteRight(self, extra=None):
        """
        handle things when the satellite is on the right
        """
        if self.ui.satelliteNameEntry.text() == '':
            self.ui.satelliteNameEntry.setText('satellite')
        print "extra", extra
        if extra is not None:
            extra.setText(0, self.ui.satelliteNameEntry.text())
        else:
            self.ui.newInstrumentButton.connect(self.ui.newInstrumentButton,
                                                QtCore.SIGNAL('clicked()'),
                                                self._newInstrument)

    def _instrumentRight(self, extra=None):
        """
        handle things when the instrument is on the right
        """
        self.ui.instrumentNameEntry.setText('instrument')
        self.ui.newProductButton.connect(self.ui.newProductButton,
                                           QtCore.SIGNAL('clicked()'),
                                           self._newProduct)

    def _processRight(self, extra=None):
        """
        handle things when the process is on the right
        """
        self.ui.processNameEntry.setText('process')
        self.ui.outputProductNameEntry.setText('output product')
        self.ui.outputTimebaseEntry.setText('output timebase')
        self.ui.extraParamsEntry.setText('extra params')
        self.ui.newCodeButton.connect(self.ui.newCodeButton,
                                           QtCore.SIGNAL('clicked()'),
                                           self._newCode)

    def _productRight(self, extra=None):
        """
        handle things when the product is on the right
        """
        self.ui.productNameEntry.setText('productNameEntry')
        self.ui.relativePathEntry.setText('relativePathEntry')
        self.ui.formatEntry.setText('formatEntry')
        self.ui.levelEntry.setValue(1.0)
        self.ui.newInspectorButton.connect(self.ui.newInspectorButton,
                                           QtCore.SIGNAL('clicked()'),
                                           self._newInspector)

    def _inspectorRight(self, extra=None):
        """
        handle things when the inspector is on the right
        """
        self.ui.inspectorNameEntry.setText('inspectorNameEntry')
        self.ui.inspectorRelativePathEntry.setText('inspectorRelativePathEntry')
        self.ui.inspectorDescriptionEntry.setText('inspectorDescriptionEntry')
        self.ui.inspectorVersionEntry.setText('inspectorVersionEntry')
        self.ui.inspectorOutputInterfaceEntry.setValue(1)
        self.ui.inspectorActiveCodeEntry.setChecked(True)
        self.ui.inspectorDateWrittenEntry.setDate(datetime.date(2000, 1, 1))
        self.ui.inspectorNewestVersionEntry.setChecked(True)
        self.ui.inspectorArgumentsEntry.setText('inspectorArgumentsEntry')

    def mouseClickEvent(self, event):
            print "event"

    def _populateExisting(self):
        """
        populate existing information into the tree
        """


def usage():
    """
    print out a usage message
    """
    print "usage: " + sys.argv[0] + " <mission name>"

if __name__ == "__main__":
    import subprocess
    subprocess.check_call(['pyside-uic', 'MainUI.ui', '-o', 'MainUI.py'])
    if len(sys.argv) != 2:
        usage()
        sys.exit()
    app = QtGui.QApplication(sys.argv)
    myapp = MyMainWindow()
    myapp.show()
    sys.exit(app.exec_())


#QtCore.Qt.Checked
#item_0 = QtGui.QTreeWidgetItem(self.treeWidget)
#item_1 = QtGui.QTreeWidgetItem(item_0)
#item_2 = QtGui.QTreeWidgetItem(item_1)
#item_1 = QtGui.QTreeWidgetItem(item_0)
