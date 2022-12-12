import json
import sys
import time
import math
import traceback
import pandas as pd
import PyQt6.QtGui as QtGui
from PyQt6.QtGui import QIcon
from PyQt6.QtWidgets import QApplication, QCheckBox, QHBoxLayout, QLabel, QMainWindow, QSpinBox, QPushButton, QScrollArea, QVBoxLayout, QWidget, QLineEdit, QTableWidget, QTableWidgetItem, QProgressBar, QHeaderView, QFileDialog, QDialog, QFormLayout, QComboBox, QMessageBox
from PyQt6.QtCore import QThread, pyqtSignal, QSize

class ThreadDataProcess(QThread):
    def __init__(self, view):
        super().__init__()
        self.view = view

    def run(self):
        self.view.page_filter()

class ThreadDataInit(QThread):
    signal_complete = pyqtSignal(int)

    def __init__(self, view):
        super().__init__()
        self.view = view

        self.signal_complete.connect(self.view.slot_data_init_complete)

    def run(self):
        self.signal_complete.emit(self.view.data_init())


class FunctionDialog(QDialog):
    signal_configuring = pyqtSignal()

    def __init__(self, parent, width = 600, height = 400):
        super().__init__(parent)
        self.resize(QSize(width, height))

    def mouseMoveEvent(self, event: QtGui.QMouseEvent):
        self.signal_configuring.emit()


class View(QMainWindow):
    ########## 信号 ##########
    # 进度更新信号
    signal_progress_update = pyqtSignal(int)
    signal_error = pyqtSignal(str)


    def __init__(self):
        super().__init__()

        # 线程
        self.is_abort = False
        self.thread_data_process = ThreadDataProcess(self)
        self.thread_data_init = ThreadDataInit(self)

        # 当日日期
        self.date = time.strftime("%Y%m%d", time.localtime())

        # 状态
        self.is_initialized = False

        # 数据文件名
        self.data_struct_path = ""
        self.data_path = ""
        self.current_data_path = "./"
        self.current_data_struct_path = "./"

        # 数据
        self.page_num = -1
        ## 保存当前条件及数据下当前页面所读取的区域起始索引
        ### 索引为 page_num - 1
        ### 值为区域起始索引，每个区域值单位大小为1000
        self.page_sections = []
        ### 保存当前条件及数据下当前页面行数
        # self.page_line_amounts = []
        ### 单页数据容量
        self.page_capacity = 1000
        ## 起始区域索引
        self.begin_section = 1
        ## 终止区域索引
        self.end_section = 1
        ## 当前区域索引
        self.current_section = 0

        ## 当前页面行数
        self.current_line_num = 0

        ## 文件是否存在表头
        self.has_header = False
        ## 文件总行数
        self.max_line_num = 0
        ## 文件最大索引
        self.max_section = 0

        self.data = {}
        self.data_view = {}
        self.data_struct = {}
        self.table_labels = []

        # 筛选条件
        self.dict_str_condition = {
            "=": lambda df, target, index: df[df[list(self.data_struct.keys())[index]] == target],
            "≠": lambda df, target, index: df[df[list(self.data_struct.keys())[index]] != target],
            "≤": lambda df, target, index: df[df[list(self.data_struct.keys())[index]] <= target],
            "≥": lambda df, target, index: df[df[list(self.data_struct.keys())[index]] >= target],
            "<": lambda df, target, index: df[df[list(self.data_struct.keys())[index]] < target],
            ">": lambda df, target, index: df[df[list(self.data_struct.keys())[index]] > target],
            "between": self.between_expression_parse,
            "contain": lambda df, target, index: df[df[list(self.data_struct.keys())[index]].str.contains(target) != 0]
        }
        self.dict_decimal_condition = {
            "=": lambda df, target, index: df[abs(df[list(self.data_struct.keys())[index]] - target) <= 0.00001],
            "≠": lambda df, target, index: df[abs(df[list(self.data_struct.keys())[index]] - target) > 0.00001],
            "≤": lambda df, target, index: df[df[list(self.data_struct.keys())[index]] <= target],
            "≥": lambda df, target, index: df[df[list(self.data_struct.keys())[index]] >= target],
            "<": lambda df, target, index: df[df[list(self.data_struct.keys())[index]] < target],
            ">": lambda df, target, index: df[df[list(self.data_struct.keys())[index]] > target],
            "between": self.between_expression_parse,
        }
        # 筛选条件缓存
        self.checkbox_select_all = QCheckBox()
        self.checkbox_select_all.setChecked(True)

        self.combobox_condition_cache = {}
        self.input_condition_cache = {}
        self.checkbox_condition_cache = {}
        self.label_condition_cache = {}

        self.combobox_condition_recover_cache = {}
        # 保存 index
        self.input_condition_recover_cache = {}
        self.checkbox_condition_recover_cache = {}
        self.label_condition_recover_cache = {}

        self.basic_data_type = ["int32", "int64", "float64", "str"]
        self.combobox_edit_cache = {}
        self.combobox_edit_recover_cache = {}

        ## 对话框
        ### 文件对话框
        self.dialog_data_file = QFileDialog()
        self.dialog_data_struct_file = QFileDialog()

        ## 表格
        ### 隐藏列名
        self.table_hidden_column = []
        ### 数据表格
        self.table_widget = QTableWidget()

        # 界面设计
        ## 主界面
        self.widget_center = QWidget(self)
        ## 按钮
        ### 打开文件
        #### 数据结构文件
        self.button_open_data_struct_file = QPushButton("Open...")
        self.button_open_data_struct_file.setToolTip("打开一个JSON格式的数据结构文件")
        #### 数据文件
        self.button_open_file = QPushButton("Open...")
        self.button_open_file.setToolTip("打开一个csv格式的数据文件，需与数据结构匹配")
        ### 生成数据结构
        self.button_generate_data_struct = QPushButton("Generate")
        self.button_generate_data_struct.setToolTip("根据csv数据文件表头生成数据结构")
        #### 根据是否存在表头设置
        self.button_generate_data_struct.setEnabled(self.has_header)
        ### 编辑数据结构
        self.button_edit_data_struct = QPushButton("Edit")
        self.button_edit_data_struct.setToolTip("编辑数据结构")
        ### 刷新
        self.button_refresh = QPushButton("Refresh")
        self.button_refresh.setToolTip("刷新数据")
        ### 筛选数据
        self.button_filter = QPushButton("Filter")
        self.button_filter.setToolTip("设定字段筛选条件")
        ### 终止筛选与数据读取
        self.button_abort = QPushButton("Abort")
        self.button_abort.setToolTip("仅解析数据时可用，终止当前操作")
        ### 翻页
        self.button_previous = QPushButton("Previous")
        self.button_previous.setToolTip("上一区域")
        self.label_page_num = QLabel("1")
        self.button_next = QPushButton("Next")
        self.button_next.setToolTip("下一区域")

        ### 筛选对话框 重置所有筛选条件
        self.button_reset_filter = QPushButton("Reset Filter")
        self.button_reset_filter.setToolTip("重置筛选条件")
        ### 筛选对话框 恢复最近一次条件筛选输入
        self.button_recover_filter = QPushButton("Recover Filter")
        self.button_recover_filter.setToolTip("还原上一次执行的筛选条件")
        ### 筛选对话框 确认执行
        self.button_execute = QPushButton("Execute")
        self.button_execute.setToolTip("执行筛选")

        ### 数据结构编辑对话框
        self.button_save = QPushButton("Save Structure")
        self.button_save.setToolTip("保存数据结构")
        self.button_recover_struct = QPushButton("Recover Structure")
        self.button_recover_struct.setToolTip("还原上一次保存的数据结构")

        ### 参数配置 设置参数
        self.button_apply_param = QPushButton("Apply Parameter")
        self.button_apply_param.setToolTip("应用参数配置（配置未修改时不会重新解析数据）")
        ### 参数配置 重置参数
        self.button_reset_param = QPushButton("Reset Parameter")
        self.button_reset_param.setToolTip("重置参数配置并应用")

        ## 单选框
        ### 数据文件是否存在表头
        self.checkbox_has_header = QCheckBox("Header")
        self.checkbox_has_header.setToolTip("数据文件是否包含表头")
        self.checkbox_has_header.setChecked(False)

        ## 标签
        self.label_status = QLabel("Ok.")

        ## 输入框
        ### 数据结构路径 输入框
        self.label_data_struct_path = QLabel("Data Struct File Path")
        self.input_data_struct_path = QLineEdit()
        ### 文件路径 输入框
        self.label_data_path = QLabel("Data File Path")
        self.input_data_path = QLineEdit()

        # 参数配置
        self.spinbox_page_capacity = QSpinBox()
        self.spinbox_page_capacity.setToolTip("每页数据参考容量（单位文件块长度）")
        self.spinbox_begin_section = QSpinBox()
        self.spinbox_begin_section.setToolTip("文件解析起始块（包含该文件块）")
        self.spinbox_end_section = QSpinBox()
        self.spinbox_end_section.setToolTip("文件解析终止块（包含该文件块）")

        self.spinbox_page_capacity.setMaximum(10000)
        self.spinbox_page_capacity.setValue(self.page_capacity)

        self.spinbox_begin_section.setMinimum(1)
        self.spinbox_begin_section.setMaximum(self.begin_section)
        self.spinbox_begin_section.setValue(self.begin_section)

        self.spinbox_end_section.setMinimum(1)
        self.spinbox_end_section.setMaximum(self.end_section)
        self.spinbox_end_section.setValue(self.end_section)

        # 数据展示
        ## 数据文件块读取进度标识
        self.label_section_progress = QLabel("0/0")
        self.label_section_progress.setToolTip("指示当前已解析的文件块索引")
        ## 文件总行数
        self.label_max_line_num = QLabel("0")
        self.label_max_line_num.setToolTip("文件行数")
        ## 当前 section 行数
        self.label_current_line_num = QLabel("0")
        self.label_current_line_num.setToolTip("当前文件块起始行数")

        # 进度条
        ## 数据处理进度
        self.progress_bar_filter = QProgressBar()

        # 布局
        ## 顶层布局
        self.layout_v_main = QVBoxLayout(self.widget_center)
        ## 基础配置布局
        self.layout_h_config = QHBoxLayout()
        ## 功能布局
        self.layout_v_function = QVBoxLayout()
        self.layout_h_function_buttons = QHBoxLayout()
        self.layout_h_function_input = QHBoxLayout()
        self.layout_h_function_data_struct_input = QHBoxLayout()
        ## 参数配置布局
        self.layout_f_param_config = QFormLayout()
        self.layout_v_param_set = QVBoxLayout()
        ## 数据展示布局
        self.layout_f_properties_show = QFormLayout()
        ## 翻页布局
        self.layout_h_page = QHBoxLayout()
        ## 进度布局
        self.layout_h_progress = QHBoxLayout()

        # 绘制
        self.draw()

        # 读取缓存
        self.cache_recover()

    # 页面属性初始化
    def page_init(self):
        # 重置分页数据
        self.page_num = 1
        self.page_sections = []
        # self.page_line_amounts = []
        self.current_section = self.begin_section - 1
        self.current_line_num = 0

        self.label_page_num.setText("1")
        # self.max_line_num = 0

    def data_struct_reset(self):
        self.combobox_condition_cache = {}
        self.input_condition_cache = {}
        self.checkbox_condition_cache = {}
        self.label_condition_cache = {}

        self.combobox_condition_recover_cache = {}
        self.input_condition_recover_cache = {}
        self.checkbox_condition_recover_cache = {}
        self.label_condition_recover_cache = {}

        self.combobox_edit_cache = {}
        self.combobox_edit_recover_cache = {}

    # 数据初始化 默认值（配置文件）
    def data_struct_init(self):
        self.data_struct = {}

        self.table_labels = []
        self.table_hidden_column = []

        # 初始化表头选项
        if self.data_struct_path == "generate_data_struct.json":
            self.has_header = True
            self.checkbox_has_header.setChecked(True)
        else:
            self.has_header = False
            self.checkbox_has_header.setChecked(False)
        # 配置文件
        ## 设置数据类型及表格表头
        with open(file=self.data_struct_path, mode='r+') as data_struct_json:
            self.data_struct = json.load(data_struct_json)
            for item in self.data_struct:
                self.table_labels.append(item)

        self.input_data_struct_path.setText(self.data_struct_path)
        self.redraw_data_related()

    def cache_recover(self):
        try:
            with open(file="./cache.json", mode='r+') as cache_json:
                cache = json.load(cache_json)
                self.data_struct_path = cache['data_struct_path']
                self.current_data_struct_path = self.data_struct_path
                self.current_data_path = cache['data_path']

                self.data_struct_reset()
                self.data_struct_init()
        except FileNotFoundError:
            self.label_status.setText("Cache recover failed.")
        except json.decoder.JSONDecodeError:
            self.label_status.setText("Cache recover failed.")

    def data_read(self, skiprows, nrows):
        if self.has_header:
            skiprows += 1
        return pd.read_csv(self.data_path, header=None, names=self.table_labels, dtype=self.data_struct, skiprows=skiprows, nrows=nrows)

    def data_init(self):
        # 数据文件
        self.data = {}
        self.data_view = {}

        try:
            self.begin_section = 1
            self.page_init()
            # 计算总行数
            self.compute_max_line_num()
            # # 读取首段数据
            # self.data = self.data_read(0, self.page_capacity)
            # self.data_view = self.data
            # 设置基础属性

            self.end_section = self.max_section

        except ValueError:
            if self.max_line_num <= 0:
                self.signal_error.emit("The data file content dose not match the data structure.")
                return -1
            else:
                self.end_section = self.max_section
                return 1
        except FileNotFoundError:
            self.signal_error.emit("Please import data file.")
            return -2

        return 0

    def table_init(self):
        if not self.is_initialized:
            return

        r = 0
        for row in self.data_view.values:
            self.table_widget.insertRow(self.table_widget.rowCount())
            c = 0
            offset = 0
            for value in row:
                if self.table_hidden_column.count(self.table_labels[c]) != 0:
                    offset += 1
                else:
                    self.table_widget.setItem(r, c - offset, QTableWidgetItem(str(value)))
                c += 1
            r += 1

    def draw(self):
        self.setWindowIcon(QIcon("./resource/csv_viewer.ico"))

        # 绘制
        # 添加主窗体
        self.setCentralWidget(self.widget_center)

        # 顶层布局
        self.widget_center.setLayout(self.layout_v_main)
        ## 基础配置布局
        self.layout_v_main.addLayout(self.layout_h_config)
        ## 表格组件
        self.layout_v_main.addWidget(self.table_widget, stretch=200)
        ## 翻页
        self.layout_v_main.addLayout(self.layout_h_page)
        ## 进度条
        self.layout_v_main.addLayout(self.layout_h_progress)
        ## 状态栏
        self.layout_v_main.addWidget(self.label_status)

        # 基础配置布局
        ## 功能布局
        self.layout_h_config.addLayout(self.layout_v_function, stretch=4)
        self.layout_h_config.addStretch(1)
        self.layout_h_config.addLayout(self.layout_f_param_config, stretch=1)
        self.layout_h_config.addLayout(self.layout_v_param_set, stretch=1)
        self.layout_h_config.addStretch(1)
        self.layout_h_config.addLayout(self.layout_f_properties_show, stretch=1)

        # 功能布局
        ## 数据结构文件路径输入框
        self.layout_v_function.addLayout(self.layout_h_function_data_struct_input)
        self.layout_h_function_data_struct_input.addWidget(self.label_data_struct_path)
        self.layout_h_function_data_struct_input.addWidget(self.input_data_struct_path)
        self.input_data_struct_path.setReadOnly(True)
        ## 打开文件按钮
        self.layout_h_function_data_struct_input.addWidget(self.button_open_data_struct_file)
        ## 编辑按钮
        self.layout_h_function_data_struct_input.addWidget(self.button_edit_data_struct)
        ## 生成数据结构按钮
        self.layout_h_function_data_struct_input.addWidget(self.button_generate_data_struct)
        ## 数据文件路径输入框
        self.layout_v_function.addLayout(self.layout_h_function_input)
        self.layout_h_function_input.addWidget(self.label_data_path)
        self.layout_h_function_input.addWidget(self.input_data_path)
        self.input_data_path.setReadOnly(True)
        ## 解析按钮
        self.layout_h_function_input.addWidget(self.button_open_file)
        ## 表头选项
        self.layout_h_function_input.addWidget(self.checkbox_has_header)

        # 功能按钮布局
        self.layout_v_function.addLayout(self.layout_h_function_buttons)
        ## 刷新按钮
        self.layout_h_function_buttons.addWidget(self.button_refresh)
        ## 筛选按钮
        self.layout_h_function_buttons.addWidget(self.button_filter)
        self.button_filter.setEnabled(False)
        ## 终止按钮
        self.layout_h_function_buttons.addWidget(self.button_abort)
        self.button_abort.setEnabled(False)

        # 参数配置布局
        self.layout_f_param_config.addRow("Page Capacity", self.spinbox_page_capacity)
        self.layout_f_param_config.addRow("Begin Section", self.spinbox_begin_section)
        self.layout_f_param_config.addRow("End Section", self.spinbox_end_section)
        self.layout_v_param_set.addWidget(self.button_apply_param)
        self.layout_v_param_set.addWidget(self.button_reset_param)
        self.layout_v_param_set.addStretch(1)
        # 数据展示布局
        self.layout_f_properties_show.addRow("Section", self.label_section_progress)
        self.layout_f_properties_show.addRow("Line", self.label_max_line_num)
        self.layout_f_properties_show.addRow("Current Line", self.label_current_line_num)

        # 翻页布局
        self.layout_h_page.addWidget(self.button_previous)
        self.layout_h_page.addStretch(2)
        self.layout_h_page.addWidget(self.label_page_num)
        self.layout_h_page.addStretch(2)
        self.layout_h_page.addWidget(self.button_next)
        self.button_previous.setEnabled(False)
        self.button_next.setEnabled(False)

        # 进度布局
        # self.layout_h_progress.addWidget(self.label_section_progress)
        self.layout_h_progress.addWidget(self.progress_bar_filter)
        self.progress_bar_filter.hide()

        # 信号槽连接
        self.communicate()

    # 更新data_struct时清空缓存
    def draw_filter_dialog(self):
        # 筛选条件对话框
        dialog_filter = FunctionDialog(self)
        dialog_filter.setModal(True)
        dialog_filter.setWindowTitle("Filter")

        widget_form = QWidget()

        # 滚动区域
        scroll_area_filter = QScrollArea()
        # 筛选对话框布局
        ## 总体布局
        layout_h_dialog = QHBoxLayout()
        ### 按钮布局
        layout_v_button = QVBoxLayout()
        ### 条件表单布局
        layout_f_filter = QFormLayout()

        # 布局配置
        dialog_filter.setLayout(layout_h_dialog)
        widget_form.setLayout(layout_f_filter)

        layout_h_dialog.addWidget(scroll_area_filter)
        layout_h_dialog.addLayout(layout_v_button)

        # 按钮
        layout_v_button.addWidget(self.button_execute)
        layout_v_button.addWidget(self.button_reset_filter)
        layout_v_button.addWidget(self.button_recover_filter)

        layout_f_filter.addRow("Show All", self.checkbox_select_all)
        layout_f_filter.addRow('', QLabel())

        # 表单
        if len(self.combobox_condition_cache) == 0:
            for (key, value) in self.data_struct.items():
                layout_h_condition = QHBoxLayout()
                checkbox_condition = QCheckBox()
                combobox_condition = QComboBox()
                input_condition = QLineEdit()
                label_condition = QLabel()

                checkbox_condition.setChecked(True)
                input_condition.setPlaceholderText(value)

                # 信号槽连接
                combobox_condition.currentTextChanged.connect(lambda text, input=input_condition, data_type=value: self.slot_between_selected(text, input, data_type))
                checkbox_condition.stateChanged.connect(lambda state, combobox=combobox_condition, input=input_condition, field=key: self.slot_field_display(state, combobox, input, field))

                layout_f_filter.addRow(key, layout_h_condition)
                layout_h_condition.addWidget(checkbox_condition)
                layout_h_condition.addWidget(combobox_condition)
                layout_h_condition.addWidget(input_condition)
                layout_f_filter.addRow('', label_condition)

                if value == "str":
                    combobox_condition.addItems(list(self.dict_str_condition.keys()))
                else:
                    combobox_condition.addItems(list(self.dict_decimal_condition.keys()))

                # 记录缓存
                self.checkbox_condition_cache[key] = checkbox_condition
                self.combobox_condition_cache[key] = combobox_condition
                self.input_condition_cache[key] = input_condition
                self.label_condition_cache[key] = label_condition
        else:
            for (key, value) in self.data_struct.items():
                layout_h_condition = QHBoxLayout()
                layout_f_filter.addRow(key, layout_h_condition)

                layout_h_condition.addWidget(self.checkbox_condition_cache[key])
                layout_h_condition.addWidget(self.combobox_condition_cache[key])
                layout_h_condition.addWidget(self.input_condition_cache[key])

                layout_f_filter.addRow('', self.label_condition_cache[key])

        ## 表单滚动
        scroll_area_filter.setWidget(widget_form)
        scroll_area_filter.setWidgetResizable(True)

        dialog_filter.open()
        ## 关闭信号绑定
        dialog_filter.rejected.connect(self.slot_exit_filter)
        ## 操作信号绑定
        dialog_filter.setMouseTracking(True)
        self.slot_configure_filter()

    def draw_data_struct_edit_dialog(self):
        # 筛选条件对话框
        dialog_edit = FunctionDialog(self, 400, 400)
        dialog_edit.setModal(True)
        dialog_edit.setWindowTitle("Data Struct Editor")

        widget_form = QWidget()

        # 滚动区域
        scroll_area_filter = QScrollArea()
        # 筛选对话框布局
        ## 总体布局
        layout_h_dialog = QHBoxLayout()
        ### 按钮布局
        layout_v_button = QVBoxLayout()
        ### 条件表单布局
        layout_f_edit = QFormLayout()

        # 布局配置
        dialog_edit.setLayout(layout_h_dialog)
        widget_form.setLayout(layout_f_edit)

        layout_h_dialog.addWidget(scroll_area_filter)
        layout_h_dialog.addLayout(layout_v_button)

        # 按钮
        layout_v_button.addWidget(self.button_save)
        layout_v_button.addWidget(self.button_recover_struct)

        # 表单
        if len(self.combobox_edit_cache) == 0:
            for (key, value) in self.data_struct.items():
                combobox_edit = QComboBox()

                combobox_edit.addItems(self.basic_data_type)
                combobox_edit.setCurrentText(value)

                layout_f_edit.addRow(key, combobox_edit)

                # 记录缓存
                self.combobox_edit_cache[key] = combobox_edit
        else:
            for (key, value) in self.data_struct.items():
                layout_f_edit.addRow(key, self.combobox_edit_cache[key])

        ## 表单滚动
        scroll_area_filter.setWidget(widget_form)
        scroll_area_filter.setWidgetResizable(True)

        dialog_edit.open()
        ## 关闭信号绑定
        dialog_edit.rejected.connect(self.slot_exit_editor)
        ## 操作信号绑定
        dialog_edit.setMouseTracking(True)
        self.slot_configure_editor()

    def redraw_table_widget(self):
        table_widget = self.table_widget

        # if self.page_num < 0:
        #     self.label_status.setText("Please import data first.")
        #     return

        ## 表格
        ### 数据表格
        self.table_widget = QTableWidget()
        self.table_widget.setRowCount(0)
        table_labels = self.table_labels.copy()

        for column in self.table_hidden_column:
            if table_labels.count(column) != 0:
                table_labels.remove(column)

        self.table_widget.setColumnCount(len(table_labels))
        self.table_widget.setHorizontalHeaderLabels(table_labels)
        self.table_widget.horizontalHeader().setSectionResizeMode(QHeaderView.ResizeMode.ResizeToContents)

        self.layout_v_main.replaceWidget(table_widget, self.table_widget)

        # 连接信号槽
        self.table_widget.cellClicked.connect(self.slot_current_line_update)

        self.table_init()

    # 初始化时调用
    def redraw_data_related(self):
        if len(self.input_data_struct_path.text()) == 0:
            self.label_status.setText("Please import data struct.")
            return
        # if len(self.input_data_path.text()) == 0:
        #     self.redraw_table_widget()
        #     self.label_status.setText("Please import data file.")
        #     return

        if self.progress_bar_filter.isHidden():
            self.progress_bar_filter.show()
        self.progress_bar_filter.setMaximum(0)

        self.data_path = self.input_data_path.text()

        self.enable_buttons(False)
        self.thread_data_init.start()
        # self.run_page_filter()
        self.label_status.setText("Importing data...")

    def enable_buttons(self, state):
        self.button_next.setEnabled(state)
        self.button_previous.setEnabled(state)

        # self.button_filter.setEnabled(state)
        self.button_refresh.setEnabled(state)
        self.button_abort.setEnabled(not state)
        self.button_open_data_struct_file.setEnabled(state)
        self.button_generate_data_struct.setEnabled(state and self.has_header)
        self.button_edit_data_struct.setEnabled(state)
        self.button_open_file.setEnabled(state)

        self.button_execute.setEnabled(state)
        self.button_recover_filter.setEnabled(state)
        self.button_reset_filter.setEnabled(state)

        self.button_apply_param.setEnabled(state)
        self.button_reset_param.setEnabled(state)

        self.checkbox_has_header.setEnabled(state)

    def communicate(self):
        # 文件对话框
        ## 数据结构
        self.button_open_data_struct_file.clicked.connect(self.slot_data_struct_file_select)
        ## 生成数据结构
        self.button_generate_data_struct.clicked.connect(self.slot_generate_data_struct)
        ## 编辑数据结构
        self.button_edit_data_struct.clicked.connect(self.draw_data_struct_edit_dialog)
        ## 选择数据结构后按钮可用
        self.input_data_struct_path.textChanged.connect(lambda text: self.button_generate_data_struct.setEnabled(True))
        self.input_data_struct_path.textChanged.connect(lambda text: self.button_edit_data_struct.setEnabled(True))
        ## 数据
        self.button_open_file.clicked.connect(self.slot_file_select)
        ## 存在表头
        self.checkbox_has_header.stateChanged.connect(self.slot_has_header)

        # 编辑对话框
        ## 保存
        self.button_save.clicked.connect(self.slot_save_struct)
        ## 恢复
        self.button_recover_struct.clicked.connect(self.slot_recover_struct)

        # 打开筛选条件对话框
        self.button_filter.clicked.connect(self.draw_filter_dialog)
        ## 选择数据结构后按钮可用
        self.input_data_struct_path.textChanged.connect(lambda text: self.button_filter.setEnabled(True))

        # 刷新数据
        self.button_refresh.clicked.connect(self.slot_execute_filter)

        # 筛选对话框
        ## 全选按钮
        self.checkbox_select_all.stateChanged.connect(self.slot_select_all)
        ## 执行条件筛选
        self.button_execute.clicked.connect(self.slot_execute_filter)
        ## 重置
        self.button_reset_filter.clicked.connect(self.slot_reset_filter)
        ## 恢复
        self.button_recover_filter.clicked.connect(self.slot_recover_filter)

        # 终止数据读取
        self.button_abort.clicked.connect(self.slot_abort)

        # 应用参数配置并重新读取数据
        self.button_apply_param.clicked.connect(self.slot_apply_param)
        # 重置参数配置
        self.button_reset_param.clicked.connect(self.slot_reset_param)

        # 翻页
        self.button_next.clicked.connect(self.slot_page_down)
        self.button_previous.clicked.connect(self.slot_page_up)

        # 数据相关信号槽
        self.thread_data_process.finished.connect(self.slot_page_filter_finished)
        self.signal_progress_update.connect(self.slot_progress_update)

        # 异常处理
        self.signal_error.connect(self.slot_error_handle)


    # def communicate_data_related(self):
    #     for checkbox in self.checkboxes_field:
    #         checkbox.stateChanged.connect(lambda status, text=checkbox.text(): self.slot_field_status(status, text))

    ## 选择数据结构文件
    def slot_data_struct_file_select(self):
        file_path = self.dialog_data_struct_file.getOpenFileName(self, "Open file", self.current_data_struct_path, "Json (*.json)")[0]
        if len(file_path) == 0:
            return

        self.data_struct_path = file_path

        self.data_struct_reset()
        self.data_struct_init()

        # 缓存
        with open(file="./cache.json", mode='w+') as cache_json:
            json.dump(obj={'data_struct_path': self.data_struct_path, 'data_path': self.data_path}, fp=cache_json)

        # self.redraw_data_related()
        self.current_data_struct_path = file_path

    ## 选择数据文件
    def slot_file_select(self):
        file_path = self.dialog_data_file.getOpenFileName(self, "Open file", self.current_data_path, "Excel (*.csv)")[0]
        if len(file_path) == 0:
            return

        self.input_data_path.setText(file_path)
        self.redraw_data_related()

        # 缓存
        with open(file="./cache.json", mode='w+') as cache_json:
            json.dump(obj={'data_struct_path': self.data_struct_path, 'data_path': self.data_path}, fp=cache_json)

        self.current_data_path = file_path

    def slot_has_header(self, state):
        self.has_header = state
        self.button_generate_data_struct.setEnabled(state)

        if self.max_line_num > 0:
            if state:
                self.max_line_num -= 1
            else:
                self.max_line_num += 1

        self.max_section = math.ceil(self.max_line_num / self.page_capacity)
        self.label_max_line_num.setText(str(self.max_line_num))

    def slot_execute_filter(self):
        # 重置所有异常提示
        for label in self.label_condition_cache.values():
            label.setText('')

        if not self.is_initialized:
            self.label_status.setText("Please import data file first.")
            return

        # self.page_init()
        self.slot_apply_param()

        # self.thread_data_process.start()
        self.run_page_filter()
        self.label_status.setText("Filtering...")

    def slot_exit_filter(self):
        self.label_status.setText("Finished.")

    def slot_configure_filter(self):
        self.label_status.setText("Configuring filter...")

    def slot_exit_editor(self):
        self.label_status.setText("Finished.")

    def slot_configure_editor(self):
        self.label_status.setText("Configuring editor...")

    def slot_current_line_update(self, row, column):
        self.label_current_line_num.setText(str(self.current_line_num + row))

    def slot_between_selected(self, text, input, data_type):
        if text == "between":
            input.setPlaceholderText('例如：(' + data_type + ',' + data_type + ']')
        else:
            input.setPlaceholderText(str(data_type))

    def slot_field_display(self, state, combobox, input, field):
        # 条件选项配置是否可用
        combobox.setEnabled(state)
        input.setEnabled(state)

        # 字段筛选
        if state:
            self.table_hidden_column.remove(field)
        else:
            self.table_hidden_column.append(field)

    def slot_select_all(self, state):
        for checkbox in self.checkbox_condition_cache.values():
            checkbox.setChecked(state)

    def slot_reset_filter(self):
        self.checkbox_select_all.setChecked(True)

        for key in self.data_struct.keys():
            # self.checkbox_condition_cache[key].setChecked(True)
            self.combobox_condition_cache[key].setCurrentIndex(0)
            self.input_condition_cache[key].setText('')

    def slot_recover_filter(self):
        for key in self.data_struct.keys():
            if len(self.checkbox_condition_recover_cache) > 0:
                self.checkbox_condition_cache[key].setChecked(self.checkbox_condition_recover_cache[key])
            if len(self.combobox_condition_recover_cache) > 0:
                self.combobox_condition_cache[key].setCurrentIndex(self.combobox_condition_recover_cache[key])
            if len(self.input_condition_recover_cache) > 0:
                self.input_condition_cache[key].setText(self.input_condition_recover_cache[key])

    def slot_save_struct(self):
        for key in self.data_struct.keys():
            self.data_struct[key] = self.combobox_edit_cache[key].currentText()
            self.combobox_edit_recover_cache[key] = self.data_struct[key]
            # 筛选界面占位符修改
            if len(self.input_condition_cache) > 0:
                self.input_condition_cache[key].setPlaceholderText(self.data_struct[key])

        with open(self.data_struct_path, mode="w+") as data_struct_file:
            json.dump(obj=self.data_struct, fp=data_struct_file)

        self.data_struct_init()

    def slot_recover_struct(self):
        if len(self.combobox_edit_recover_cache) > 0:
            for (key, value) in self.data_struct.items():
                self.combobox_edit_cache[key].setCurrentText(self.combobox_edit_recover_cache[key])

    def slot_page_down(self):
        if self.current_section >= self.max_section:
            self.label_status.setText("Already the last page.")
            return

        self.page_num += 1
        self.label_page_num.setText(str(self.page_num))

        if len(self.page_sections) >= self.page_num:
            self.current_section = self.page_sections[self.page_num-1]
            # self.current_line_num = self.page_line_amounts[self.page_num - 1]


        # self.page_filter()
        # self.redraw_table_widget()
        self.run_page_filter()

    def slot_page_up(self):
        if self.page_num == 1:
            self.label_status.setText("Already the first page.")
            return

        self.page_num -= 1
        self.label_page_num.setText(str(self.page_num))

        self.current_section = self.page_sections[self.page_num-1]
        # self.current_line_num = self.page_line_amounts[self.page_num-1]

        # self.thread_data_process.start()
        self.run_page_filter()

    def slot_page_filter_finished(self):
        # 界面复位
        self.progress_bar_filter.setMaximum(1)
        self.progress_bar_filter.setValue(1)

        self.enable_buttons(True)

        self.redraw_table_widget()
        self.label_status.setText("Finished.")

        self.is_abort = False

    def slot_data_init_complete(self, errcode):
        self.page_init()
        if errcode < 0:
            self.is_initialized = False
            self.redraw_table_widget()
            self.progress_bar_filter.hide()
            self.enable_buttons(True)
            return
        if errcode > 0:
            QMessageBox.warning(self, "Warning", "The data file contains incomplete lines.")
        self.is_initialized = True
        self.spinbox_page_capacity.setValue(self.page_capacity)
        self.spinbox_begin_section.setMaximum(self.max_section)
        self.spinbox_begin_section.setValue(self.begin_section)
        self.spinbox_end_section.setMaximum(self.max_section)
        self.spinbox_end_section.setValue(self.end_section)

        self.run_page_filter()

        # self.button_previous.setEnabled(True)
        # self.button_next.setEnabled(True)

    def slot_progress_update(self, row_count):
        self.progress_bar_filter.setValue(row_count)
        self.label_section_progress.setText(str(self.current_section) + '/' + str(self.max_section))
        self.label_max_line_num.setText(str(self.max_line_num))
        if self.current_section > 0:
            self.current_line_num = (self.page_num - 1) * self.page_capacity + 1
            self.label_current_line_num.setText(str(self.current_line_num))

    def slot_error_handle(self, error_info):
        self.label_status.setText(error_info)

    def slot_abort(self):
        self.is_abort = True

    def slot_apply_param(self):
        if self.page_capacity != self.spinbox_page_capacity.value():
            self.page_capacity = self.spinbox_page_capacity.value()
            self.redraw_data_related()
        elif self.begin_section != self.spinbox_begin_section.value() or self.end_section != self.spinbox_end_section.value():
            if self.spinbox_begin_section.value() > self.spinbox_end_section.value():
                self.label_status.setText("Bad parameters. The begin section should be equal or less than the end section.")
                return
            self.begin_section = self.spinbox_begin_section.value()
            self.end_section = self.spinbox_end_section.value()

            self.run_page_filter()

        self.page_init()
        self.label_status.setText("Finished.")

    def slot_reset_param(self):
        self.page_capacity = 1000
        self.begin_section = 1
        self.end_section = self.max_section

        self.spinbox_page_capacity.setValue(self.page_capacity)
        self.spinbox_begin_section.setValue(self.begin_section)
        self.spinbox_end_section.setValue(self.end_section)

        self.slot_apply_param()

    def slot_generate_data_struct(self):
        if len(self.input_data_path.text()) == 0:
            self.label_status.setText("Please import data file first.")
            return
        try:
            header = list(pd.read_csv(self.input_data_path.text(), nrows=0))
            sample = list(pd.read_csv(self.input_data_path.text(), nrows=1).dtypes)

            # 设置表头
            data_struct = {}
            for i in range(len(header)):
                if sample[i] == 'O':
                    data_struct[header[i]] = "str"
                else:
                    data_struct[header[i]] = str(sample[i])

            with open("generate_data_struct.json", mode="w") as generate_file:
                json.dump(obj=data_struct, fp=generate_file)

            self.current_data_struct_path = "generate_data_struct.json"
            self.data_struct_path = "generate_data_struct.json"
            # 缓存
            with open(file="./cache.json", mode='w+') as cache_json:
                json.dump(obj={'data_struct_path': self.data_struct_path, 'data_path': self.data_path}, fp=cache_json)

            self.data_struct_reset()
            self.data_struct_init()

        except FileNotFoundError:
            self.label_status.setText("Path error.")

    # 工具方法
    ## 计算文件总行数
    def compute_max_line_num(self):
        self.begin_section = 1
        self.max_section = 0
        self.max_line_num = 0
        chunks = pd.read_csv(self.data_path, header=None, names=self.table_labels, dtype=self.data_struct, iterator=True, chunksize=self.page_capacity)
        for chunk in chunks:
            self.max_line_num += len(chunk.index)
            self.max_section += 1
            self.signal_progress_update.emit(0)

        if self.has_header and self.max_line_num > 0:
            self.max_line_num -= 1
            self.max_section = math.ceil(self.max_line_num / self.page_capacity)

    def run_page_filter(self):
        # 界面初始化
        self.enable_buttons(False)
        self.is_abort = False

        # 重置进度条
        self.progress_bar_filter.setMaximum(self.page_capacity)
        self.progress_bar_filter.setValue(0)

        # 启动线程
        self.thread_data_process.start()
        self.label_status.setText("Filtering...")

    ## 执行条件筛选（分页）
    def page_filter(self):
        if not self.is_initialized:
            return

        combobox_condition_cache_values = list(self.combobox_condition_cache.values())
        input_condition_cache_values = list(self.input_condition_cache.values())
        data_fields = list(self.data_struct.keys())
        data_types = list(self.data_struct.values())

        row_count = 0
        while not self.is_abort and row_count < self.page_capacity and self.current_section < self.end_section:
            data_view = self.data_read(self.current_section * self.page_capacity, self.page_capacity)

            if len(data_view) == 0:
                break

            if len(self.page_sections) < self.page_num:
                self.page_sections.append(self.current_section)
                # self.page_line_amounts.append(row_count)

            # 逐条筛选
            for i in range(len(self.input_condition_cache)):
                # 缓存最近一次筛选条件
                self.checkbox_condition_recover_cache[data_fields[i]] = self.checkbox_condition_cache[
                    data_fields[i]].isChecked()
                self.combobox_condition_recover_cache[data_fields[i]] = self.combobox_condition_cache[
                    data_fields[i]].currentIndex()
                self.input_condition_recover_cache[data_fields[i]] = self.input_condition_cache[data_fields[i]].text()

                # 跳过禁用项
                if not input_condition_cache_values[i].isEnabled():
                    continue

                if len(input_condition_cache_values[i].text()) == 0:
                    continue

                if combobox_condition_cache_values[i].currentText() == "between":
                    if data_types[i] == "str":
                        data_view = self.dict_str_condition["between"](data_view,
                                                                       input_condition_cache_values[i].text(), i)
                    # 数值
                    else:
                        data_view = self.dict_decimal_condition["between"](data_view,
                                                                           input_condition_cache_values[i].text(), i)
                else:
                    # 字符串
                    if data_types[i] == "str":
                        data_view = self.dict_str_condition[combobox_condition_cache_values[i].currentText()](data_view,
                                                                                                              input_condition_cache_values[
                                                                                                                  i].text(),
                                                                                                              i)
                    # 数值
                    else:
                        data_view = self.dict_decimal_condition[combobox_condition_cache_values[i].currentText()](
                            data_view, float(input_condition_cache_values[i].text()), i)

            if row_count == 0:
                self.data_view = data_view
            elif len(data_view.values) > 0:
                self.data_view = pd.concat([self.data_view, data_view])

            row_count = len(self.data_view)
            self.current_section += 1

            self.signal_progress_update.emit(row_count)

    ## between表达式解析
    def between_expression_parse(self, df, expression, index):
        if len(expression) > 0:
            section_symbols = ['[', '(', ')', ']']

            left = expression[0]
            right = expression[-1]

            # 获取区间起点与终点
            section = str(expression[1:-1]).split(',')
            if len(section) != 2 or section[0] == '' or section[1] == '':
                list(self.label_condition_cache.values())[index].setText("需要区间起点值与终点值，并以 \',\' 分割")
                return df

            data_type = list(self.data_struct.values())[index]
            try:
                if data_type == "str":
                    begin = str(section[0])
                    end = str(section[1])
                else:
                    begin = float(section[0])
                    end = float(section[1])
            except ValueError:
                list(self.label_condition_cache.values())[index].setText("区间起点与终点应为数值类型(Decimal)")
                return df

            # 区间判断
            if section_symbols.count(left) == 0 or section_symbols.count(right) == 0:
                list(self.label_condition_cache.values())[index].setText("区间应以 '[]' 或 '()' 修饰")
            else:
                if left == '[':
                    df = df[df[list(self.data_struct.keys())[index]] >= begin]
                elif left == '(':
                    df = df[df[list(self.data_struct.keys())[index]] > begin]

                if right == ']':
                    df = df[df[list(self.data_struct.keys())[index]] <= end]
                elif right == ')':
                    df = df[df[list(self.data_struct.keys())[index]] < end]

                list(self.label_condition_cache.values())[index].setText('')

            return df

def dump_error(error_info):
    with open("log.txt", mode="a+") as log:
        log.write(error_info)

if __name__ == '__main__':
    app = QApplication(sys.argv)
    try:
        view = View()
        view.showMaximized()
        sys.exit(app.exec())
    except Exception:
        dump_error(traceback.format_exc())
