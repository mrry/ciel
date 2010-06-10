/* Skylight visualisation for Skywriting event streams.
   (c) 2010, Anil Madhavapeddy <anil@recoil.org>
   Adapted for live viewing by Chris Smowton <chris@smowton.net>
*/

Skyweb = function(json) {

    var radius = 7;
    var total_grid_size=800;
    var taskmap = {};
    var gfut_to_task = {};
    var padding = radius;
    var with_lines = (radius > 5);
    var last_index = 0;
    var circles_per_line = Math.floor(total_grid_size / (radius * 2 + padding)) - 1
    var colmap = {
        'BLOCKING': '#FF0000',
	'QUEUED': '#FFA500',
        'RUNNABLE': '#FFA500',
        'ASSIGNED': '#44A500',
	'COMMITTED': '#0000FF',
	'FAILED': '#000000',
	'RUNTIME_EXCEPTION_FAIL': '#000000',
	'MISSING_INPUT_FAIL': '#000000',
	'WORKER_FAIL': '#222222'
    };

    getcolour = function(col) {

	if (!colmap[col]) {
	    if(window.console) {
		console.log("Couldn't find a colour for " + col);
	    }
	    return '#DDDDDD'
	}
	else {
	    return colmap[col];
	}

    }

    var linemap = {
	'RUNNABLE': { opacity: 0.4, stroke: '#009922', 'stroke-width': 3 },
	'ASSIGNED': { opacity: 0.9, stroke: '#335522', 'stroke-width': 4 },
	'COMMITTED': { opacity: 0.2, stroke: '#000000',	'stroke-width': 1 }
    };

    getline = function(lin) {

	if(!linemap[lin]) {
	    if(window.console) {
		console.log("Couldn't find a linestyle for " + lin);
	    }
	    return {};
	}
	else {
	    return linemap[lin];
	}

    }

    var paper = Raphael(10, 1, total_grid_size, total_grid_size);

    display_index_to_coords = function(id) {
        var tx = id % circles_per_line;
        var ty = Math.floor(id / circles_per_line);
        var x = (tx + 1) * (radius * 2 + padding);
        var y = (ty + 1) * (radius * 2 + padding);
        return {
            x: x,
            y: y
        };
    };

    create_line = function(from_task, to_task) {

	var from_coords = from_task.display_coords;
	var to_coords = to_task.display_coords;
	var l = paper.path("M" + to_coords.x + " " + to_coords.y + "L" + from_coords.x + " " + from_coords.y);

    }

    add_task_ref = function(t, refname, r) {

	if(r.__ref__) {
	    var input_ref = r.__ref__;
	    if(input_ref[0] == "gfut") {
		var gfut_id = input_ref[1];
		var gfut_task = gfut_to_task[gfut_id];
		if(!gfut_task) {
		    if(window.console) {
			console.log("Warning: ignored unknown gfut " + gfut_id);
		    }
		}
		else {
		    var new_line = create_line(gfut_task, t);
		    var new_input_line = { from_gfut: gfut_id, from_task: gfut_task, line: new_line };
		    t.input_lines[refname] = new_input_line;
		    var new_output_line = { to_task: t, to_input: refname, line: new_line };
		    if(!gfut_task.output_lines[gfut_id]) {
			gfut_task.output_lines[gfut_id] = [];
		    }
		    gfut_task.output_lines[gfut_id].push(new_output_line);

		    l.attr('opacity', '0.1');
		    l.toBack();
		}
	    }
	}
    }

    create_task = function(t, is_onload) {

	taskmap[t.task_id] = t;

	for(var o in t.expected_outputs) {

	    var this_output = t.expected_outputs[o];
	    if(gfut_to_task[this_output]) {
	        var old_task = gfut_to_task[this_output];
		if(!old_task.output_lines[this_output]) {
		    console.log("Deferring GFUT " + this_output + 
				" from task " + old_task.task_id + 
				" to " + t.task_id + " failed");
		}
		else {
		    var lines_to_replace = old_task.output_lines[this_output];
		    for(var i in lines_to_replace) {
			var this_line = lines_to_replace[i];
			// Nobble the old line
			this_line.line.remove();
			// Make a new one
			this_line.line = create_line(t, this_line.to_task);
			if(!t.output_lines[this_output]) {
			    t.output_lines[this_output] = [];
			}
			t.output_lines[this_output].push(this_line);
			// Fix metadata at the other end
			this_line.to_task.input_lines[this_line.to_input].from_task = t;
		    }
		    delete old_task.output_lines[this_output];
		}
	    }

	    gfut_to_task[t.expected_outputs[o]] = t;

	}

	t.display_index = last_index++;
	t.display_coords = display_index_to_coords(t.display_index);
	t.circle = paper.circle(t.display_coords.x, t.display_coords.y, radius);
	t.circle.attr({
		'fill': getcolour(t.state),
		'stroke-width': 1
		    });       

	t.input_lines = {};
	t.output_lines = {};
	if (with_lines) {
	    /* FIXME: Need to either check these two namespaces never clash, or fix this description
	       at the server end */
	    for (var i in t["inputs"]) {
		var input_spec = t["inputs"][i];
		add_task_ref(t, i, input_spec);
	    }
	    for (var i in t["dependencies"]) {
		var input_spec = t["dependencies"][i];
		add_task_ref(t, i, input_spec);
	    }
	}
    }

    for (var t in json.taskmap) {

	create_task(json.taskmap[t], true);

    }

    var next_event_index = json.evtcount;
    // Actually some of the taskmap might reflect later state; each task ships with an evtcount letting us know of that.

    notify_events_callback = function(json) {

	if(json.exited) {

	    alert("The server reported it had halted");

	}
	else {

	    $.getJSON("../task/events/" + next_event_index, process_events_callback);

	}

    }

    process_events_callback = function(json) {
	
	for(var i in json.events) {

	    var ev = json.events[i]
	    var act = ev.action;

	    if(!taskmap[ev.task_id]) {
		if(act != 'CREATED') {
		    if(window.console) {
			console.log('Unexpected act ' + act + ' for task ' + ev.task_id);
		    }
		}
		else {
		    ev.task_descriptor.task_id = ev.task_id;
		    ev.task_descriptor.state = ev.initial_state;
		    ev.task_descriptor.event_index = ev.index;
		    create_task(ev.task_descriptor, false);
		}
	    }
	    else {

		var taskd = taskmap[ev.task_id];
		
		if(taskd.event_index < ev.index) {
		    taskd.circle.animate({'fill': getcolour(ev.action)}, 200);
		    for (x in taskd.input_lines) {
			var lin = taskd.input_lines[x].line;
			lin.animate(getline(ev.action), 200);
		    }
		    taskd.event_index = ev.index;
		}

	    }

	}

	next_event_index = json.next_event_to_fetch;

	if(json.next_event_to_fetch < json.first_unavailable_event) {

	    $.getJSON("../task/events/" + json.next_event_to_fetch, process_events_callback);
	    
	}
	else {

	    $.getJSON("../task/wait_event_count/" + json.next_event_to_fetch, notify_events_callback);

	}

    };

    $.getJSON("../task/wait_event_count/" + next_event_index, notify_events_callback);

};
