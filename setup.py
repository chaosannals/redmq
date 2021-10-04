from cx_Freeze import setup, Executable

build_exe_options = {
    "packages": ["os"],
    "excludes": ["tkinter"]
}

setup(
    name = "redmq",
    version = "0.1.0",
    description = "yet a redis mq server",
    executables = [
        Executable(
            "app.py",
            copyright="Copyright (C) 2021 ChaosAnnals",
        ),
    ],
    options = {
        "build_exe": build_exe_options
    }
)