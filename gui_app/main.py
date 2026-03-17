import sys

from PyQt5.QtCore import QCoreApplication, Qt
from PyQt5.QtGui import QFont
from PyQt5.QtWidgets import QApplication

from gui_app.main_window import MainWindow
from gui_app.theme import apply_theme


def main():
    QCoreApplication.setAttribute(Qt.AA_ShareOpenGLContexts)
    app = QApplication(sys.argv)
    from PyQt5.QtWebEngineWidgets import QWebEngineView
    _ = QWebEngineView

    app.setApplicationName("EasyXT量化交易策略管理平台")
    app.setApplicationVersion("3.0")
    app.setOrganizationName("EasyXT")
    app.setFont(QFont("Microsoft YaHei", 9))
    apply_theme(app, "dark")

    window = MainWindow()
    window.show()
    sys.exit(app.exec_())


if __name__ == "__main__":
    main()
