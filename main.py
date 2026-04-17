import customtkinter as ctk
from logger_setup import setup_global_exception_handler
from gui import ParserApp


def main():
    setup_global_exception_handler()
    root = ctk.CTk()
    app = ParserApp(root)
    root.protocol("WM_DELETE_WINDOW", app.on_closing)
    root.mainloop()


if __name__ == "__main__":
    main()
