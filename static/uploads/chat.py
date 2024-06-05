import sys
from PySide6.QtWidgets import QApplication, QWidget, QVBoxLayout, QHBoxLayout, QLabel, QScrollArea, QPushButton
from PySide6.QtCore import Qt, QRect
from qfluentwidgets import ScrollArea


class ChatBubble(QWidget):
    def __init__(self, message, is_sender, parent=None):
        super().__init__(parent)
        self.is_sender = is_sender
        self.initUI(message)

    def initUI(self, message):
        self.layout = QHBoxLayout()

        self.message_label = QLabel(message)
        self.message_label.setWordWrap(True)
        self.message_label.setMargin(10)

        if self.is_sender:
            # 发送者的消息气泡样式
            self.message_label.setStyleSheet("background-color: lightblue; border-radius: 15px; padding: 10px;")
            self.layout.addStretch(1)
            self.layout.addWidget(self.message_label, alignment=Qt.AlignRight)
        else:
            # 接收者的消息气泡样式
            self.message_label.setStyleSheet("background-color: lightgrey; border-radius: 15px; padding: 10px;")
            self.layout.addWidget(self.message_label, alignment=Qt.AlignLeft)
            self.layout.addStretch(1)

        self.setLayout(self.layout)


class ChatWindow(QWidget):
    def __init__(self):
        super().__init__()
        self.initUI()

    def initUI(self):
        self.setWindowTitle("Chat Window")
        self.setGeometry(QRect(10, 60, 621, 241))

        # 创建滚动区域
        self.scroll_area = ScrollArea()
        self.chat_layout = QVBoxLayout()

        # 将滚动区域的布局设置为垂直
        self.chat_layout.setContentsMargins(0, 0, 0, 0)
        self.chat_layout.setSpacing(5)

        # 创建消息显示区域
        self.chat_widget = QWidget()
        self.chat_widget.setLayout(self.chat_layout)

        # 将消息显示区域添加到滚动区域中
        self.scroll_area.setWidget(self.chat_widget)
        #self.scroll_area.ensureVisible(-100,-100)

        # 初始化一些示例消息
        self.addMessage("Hello, how are you?", is_sender=False)
        self.addMessage("I'm fine, thank you! And you?", is_sender=True)
        self.addMessage("Hello, how are you?", is_sender=False)
        self.addMessage("I'm fine, thank you! And you?", is_sender=True)
        self.addMessage("Hello, how are you?", is_sender=False)
        self.addMessage("I'm fine, thank you! And you?", is_sender=True)

        # 设置主布局并添加滚动区域
        self.main_layout = QVBoxLayout()
        self.main_layout.addWidget(self.scroll_area)
        self.setLayout(self.main_layout)

    def addMessage(self, message, is_sender):
        chat_bubble = ChatBubble(message, is_sender)
        self.chat_layout.addWidget(chat_bubble)
        self.scroll_area.ensureWidgetVisible(chat_bubble, 0)


if __name__ == "__main__":
    app = QApplication([])
    window = ChatWindow()
    window.show()
    sys.exit(app.exec())
