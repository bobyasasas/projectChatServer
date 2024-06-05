import sys

from PySide6.QtCore import QUrl
from qfluentwidgets import MessageBoxBase, SubtitleLabel, LineEdit


class CustomMessageBox(MessageBoxBase):

    def __init__(self, parent=None):
        super().__init__(parent)
        self.titleLabel = SubtitleLabel('添加好友', self)
        self.urlLineEdit = LineEdit(self)

        self.urlLineEdit.setPlaceholderText('输入好友的用户名')
        self.urlLineEdit.setClearButtonEnabled(True)

        self.viewLayout.addWidget(self.titleLabel)
        self.viewLayout.addWidget(self.urlLineEdit)

        # change the text of button
        self.yesButton.setText('添加')
        self.cancelButton.setText('取消')

        self.widget.setMinimumWidth(350)
        self.yesButton.setDisabled(True)
        self.urlLineEdit.textChanged.connect(self._validateUrl)

        # self.hideYesButton()

    def _validateUrl(self, text):
        self.yesButton.setEnabled(QUrl(text).isValid())
