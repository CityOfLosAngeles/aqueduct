def _jupyter_nbextension_paths():
    return [
        {
            "section": "notebook",
            "src": "static",
            "dest": "civis_aqueduct_utils",
            "require": "civis_aqueduct_utils/extension",
        }
    ]
