
skypy_result = spawn_other("skypy", {"pyfile_ref": package("skypy_main"), "entry_point": "skypy_callback", "entry_args": ["Hello from SW"], "export_json": true, "small_task": true});

return ["SW returning", *(skypy_result)];