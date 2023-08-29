import tkinter as tk


class FormTemplate(object):

    def __init__(self, sizex=500, sizey=250, title=None, run_button_name=None, run_button_action: str = None):
        self.root = tk.Tk()
        # self.root.resizable(False, False)
        self.row = 10

        # Parts
        self.top_frame = tk.Frame(self.root)
        self.canvas = tk.Canvas(self.root, width=sizex, height=sizey)
        self.main_frame = tk.Frame(self.root)
        self.bottom_frame = tk.Frame(self.root)

        self.top_frame.pack()

        # Make scrollbars bound to root and scrolling form canvas
        h_bar = tk.Scrollbar(self.root, orient=tk.HORIZONTAL)
        h_bar.pack(side=tk.BOTTOM, fill=tk.X)
        h_bar.config(command=self.canvas.xview)
        v_bar = tk.Scrollbar(self.root, orient=tk.VERTICAL)
        v_bar.pack(side=tk.RIGHT, fill=tk.Y)
        v_bar.config(command=self.canvas.yview)
        self.canvas.configure(xscrollcommand=h_bar.set, yscrollcommand=v_bar.set)

        # Position Canvas and Form frame
        self.canvas.pack(anchor=tk.NW)
        self.canvas.create_window(0.015 * sizex, 0.1 * sizey, window=self.main_frame, anchor=tk.NW)
        self.canvas.bind("<Configure>", lambda event: self.canvas.configure(scrollregion=self.canvas.bbox("all")))

        if title:
            self.add_title(title)

        # Place buttons frame
        self.bottom_frame.pack(anchor=tk.SE, expand=0)  # , fill=tk.Y, expand=True)

        self.run_button = None
        if run_button_name or run_button_action:
            self.run_button = self.set_action_button(run_button_action, run_button_name)
        else:
            self.run_button = self.set_action_button("Run", None)

        self.add_button("Exit", button_action=lambda: self.root.destroy(), side=tk.LEFT)

        self.root.bind('<Escape>', lambda event: self.root.destroy())

    def open(self):
        self.root.mainloop()

    def add_text_field(self, name="Field Name", default="Enter Value", info=None):
        tk.Label(self.main_frame, text=name).grid(row=self.row, column=10, sticky=tk.W, padx=5, ipadx=5)
        var = tk.StringVar()
        var.set(default)
        tk.Entry(self.main_frame, textvariable=var, width=30).grid(row=self.row, column=20, sticky=tk.W)
        tk.Label(self.main_frame, text=info).grid(row=self.row, column=30, sticky=tk.W)
        self.row += 10
        return var

    def add_num_field(self, name="Field Name", default=10, info=None):
        tk.Label(self.main_frame, text=name).grid(row=self.row, column=10, sticky=tk.W, padx=5, ipadx=5)
        var = tk.IntVar()
        var.set(default)
        tk.Entry(self.main_frame, textvariable=var, width=30).grid(row=self.row, column=20, sticky=tk.W)
        tk.Label(self.main_frame, text=info).grid(row=self.row, column=30, sticky=tk.W)
        self.row += 10
        return var

    def add_bool_field(self, name="Field Name", default=True, info=None, text=''):
        tk.Label(self.main_frame, text=name).grid(row=self.row, column=10, sticky=tk.W, padx=5, ipadx=5)
        var = tk.BooleanVar()
        var.set(default)
        tk.Checkbutton(self.main_frame, text=text, variable=var, onvalue=1, offvalue=0). \
            grid(row=self.row, column=20, sticky=tk.W, padx=20, ipadx=20)
        tk.Label(self.main_frame, text=info).grid(row=self.row, column=30, sticky=tk.W)
        self.row += 10
        return var

    def add_button(self, button_name="Button", button_action=None, side=None):
        b = tk.Button(self.bottom_frame, text=button_name, command=button_action)
        b.pack(side=side)
        return b

    def add_title(self, title: str):
        self.root.title(title)

    def add_wellcome_message(self, message: str, side=None):
        tk.Label(self.top_frame, text=message).pack(pady=5, side=side)

    def set_action_button(self, name="Run", action=None):
        if self.run_button is not None:
            self.run_button.destroy()

        text = """
    \nUse <object>.set_action_button(name, lambda: func(args))
    \rUse <object>.add_title("Title") for title
    \rUse <object>.add_wellcome_message("Message", side=None) for top message
    \rUse str_param = form.add_text_field(name="name", default="default", info="info")
    \rUse int_param = form.add_int_field(name="name", default=1, info="info")
    \rUse bool_param = form.add_bool_field(name="name", default=True, info="info")
    """

        action = action or (lambda x=text: print(x))
        button_text = name
        b = self.add_button(button_text, button_action=action, side=tk.RIGHT)
        self.root.bind('<Return>', lambda event: action())
        return b


class TestForm(object):

    def __init__(self):
        pass

    @staticmethod
    def action_print(text):
        print(text)

    def test_blank_form(self):
        form = FormTemplate(title="TEST BLANK FORM")
        form.open()

    def test_filled_form(self):
        form2 = FormTemplate()

        form2.add_title("TEST MODIFIED FORM")

        top_text = """First Welcome Message
        Line 2 of first message.............................."""
        form2.add_wellcome_message(top_text)
        form2.add_wellcome_message("Second wellcome message", side=tk.LEFT)

        form2.set_action_button("Test Action", lambda: self.action_print("Test Action Button Pressed"))
        form2.add_button("non run button2", lambda: self.action_print("non run button3 pressed"), side=tk.LEFT)
        form2.add_button("non run button3", lambda: self.action_print("non run button3 pressed"), side=tk.LEFT)

        params = dict()

        params['t1'] = form2.add_text_field()
        params['t2'] = form2.add_text_field("field 1", "field1 default value", "field1 info")
        params['t3'] = form2.add_text_field(name="field2", default="field2 default value", info="text " * 25)
        params['n1'] = form2.add_num_field(name="numeric_field", default=10, info="Numeric Values")
        params['b1'] = form2.add_bool_field(name="Boolean Field", default=True, info="CheckBox", text="CHECK")
        params['b2'] = form2.add_bool_field(name="Boolean Field", default=False, info="CheckBox", text="NO CHECK")
        params['b3'] = form2.add_bool_field()

        form2.add_button("Check Params",
                         lambda: self.action_print(f"{[f'{k}: {v.get()}' for k, v in params.items()]}"),
                         side=tk.RIGHT)

        form2.open()


def main():
    pass


def test_form():
    TestForm().test_blank_form()
    TestForm().test_filled_form()


if __name__ == "__main__":
    main()
