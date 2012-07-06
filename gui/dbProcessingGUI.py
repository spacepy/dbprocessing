
## pyside-uic MainUI.ui -o MainUI.py

import sys

from PySide import QtCore, QtGui

from MainUI import Ui_MainWindow

class MyMainWindow(QtGui.QMainWindow):
    def __init__(self, parent=None):
        super(MyMainWindow, self).__init__(parent)
        # QtGui.QMainWindow.__init__(self, parent)
        self.ui = Ui_MainWindow()
        self.ui.setupUi(self)
        self.ui.RightFrame.setVisible(False)
#        print self.ui.treeWidget.topLevelItem(0).isSelected()
#        print self.ui.treeWidget.topLevelItem(0).emitDataChanged()
        self._initialTree()

    def _initialTree(self):
        """
        populate the initial tree with placeholders
        """

        item = QtGui.QTreeWidgetItem(self.ui.treeWidget.topLevelItem(0)) # satellite
        item.setText(0, QtGui.QApplication.translate("MainWindow", "Satellite", None, QtGui.QApplication.UnicodeUTF8))
        item = QtGui.QTreeWidgetItem(self.ui.treeWidget.topLevelItem(0).child(0)) # instrument
        item.setText(0, QtGui.QApplication.translate("MainWindow", "Instrument", None, QtGui.QApplication.UnicodeUTF8))
        item = QtGui.QTreeWidgetItem(self.ui.treeWidget.topLevelItem(0).child(0).child(0)) # instrument
        item.setText(0, QtGui.QApplication.translate("MainWindow", "Product", None, QtGui.QApplication.UnicodeUTF8))
        self.ui.treeWidget.connect(self.ui.treeWidget.selectionModel(),
                     QtCore.SIGNAL("selectionChanged(QItemSelection, QItemSelection)"),
                     self._debugfn)

    def _fillRightSide(self):
        """
        display the right side and get the values
        """
        self.ui.RightFrame.setVisible(True)



    def _debugfn(self):
        print self.ui.treeWidget.topLevelItem(0).isSelected()
        print self.ui.treeWidget.topLevelItem(0).child(0).isSelected()
        print self.ui.treeWidget.topLevelItem(0).child(0).child(0).isSelected()
        print self.ui.treeWidget.topLevelItem(0).child(0).child(0).child(0).isSelected()
        print()
#        print self.ui.treeWidget.topLevelItem(0).isSelected()
        self._fillRightSide()

    def mouseClickEvent(self, event):
            print "event"

if __name__ == "__main__":
    app = QtGui.QApplication(sys.argv)
    myapp = MyMainWindow()
    myapp.show()
    sys.exit(app.exec_())


QtCore.Qt.Checked
item_0 = QtGui.QTreeWidgetItem(self.treeWidget)
item_1 = QtGui.QTreeWidgetItem(item_0)
item_2 = QtGui.QTreeWidgetItem(item_1)
item_1 = QtGui.QTreeWidgetItem(item_0)