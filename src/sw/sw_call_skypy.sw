
skypy_result = spawn_other("skypy", {"pyfile_ref": package("skypy_main"), "entry_point": "skypy_callback", "entry_args": ["Hello from SW"]});

return skypy_result;