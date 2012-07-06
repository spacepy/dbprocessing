
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
        self.ui.stackedWidget.setVisible(False)
#        print self.ui.treeWidget.topLevelItem(0).isSelected()
#        print self.ui.treeWidget.topLevelItem(0).emitDataChanged()
        self.rightSide = {}
        self.rightSide['product'] = []
        self.rightSide['mission'] = []
        self.rightSide['satellite'] = []
        self.rightSide['instrument'] = []
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
        self.rightSide['product'].append(self.ui.treeWidget.topLevelItem(0).child(0).child(0).child(0))
        self.rightSide['instrument'].append(self.ui.treeWidget.topLevelItem(0).child(0).child(0))
        self.rightSide['satellite'].append(self.ui.treeWidget.topLevelItem(0).child(0))
        self.rightSide['mission'].append(self.ui.treeWidget.topLevelItem(0))

    def _fillRightSide(self):
        """
        display the right side and get the values
        """
        self.ui.stackedWidget.setVisible(True)
        if self.rightSide['mission'][0].isSelected():
            self.ui.stackedWidget.setCurrentIndex(1)
        if self.rightSide['product'][0].isSelected():
            self.ui.stackedWidget.setCurrentIndex(0)
        # is mission selected?
#        if self.rightSide['mission'][0].isSelected():
#            self.ui.MissionRight.setVisible(True)


    def _debugfn(self):
        print self.rightSide['mission'][0].isSelected()
        print self.rightSide['satellite'][0].isSelected()
        print self.rightSide['instrument'][0].isSelected()
        print self.rightSide['product'][0].isSelected()
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