/*
 * Copyright (c) 2020 City of Los Angeles
 *
 * Distributed under the terms of the Apache 2.0 License.
 *
 * The full license is in the file LICENSE, distributed with this software.
 */

define(['jquery', 'base/js/namespace'], function($, Jupyter) {
    "use strict";
    var open_lab = function() {
        Jupyter.notebook.save_notebook().then(function () {
            let lab_url = Jupyter.notebook.base_url + "lab/tree/" + Jupyter.notebook.notebook_path;
            window.location.href = lab_url
        });
    }
    var load_ipython_extension = function() {
        Jupyter.toolbar.add_buttons_group([{
            id : 'toggle_codecells',
            label : 'JupyterLab',
            icon : 'fa-desktop',
            callback : open_lab,
        }]);
    };
    return {
        load_ipython_extension : load_ipython_extension
    };
});
