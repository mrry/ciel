/* Skylight visualisation for Skywriting event streams.
   (c) 2010, Anil Madhavapeddy <anil@recoil.org>
   Adapted for live viewing by Chris Smowton <chris@smowton.net>
*/

Skyweb = function(json) {

    var radius = 7;
    var total_grid_size=800;
    var taskmap = {};
    var padding = radius;
    var with_lines = (radius > 5);
    var last_index = 0;
    var circles_per_line = Math.floor(total_grid_size / (radius * 2 + padding)) - 1
    var colmap = {
        'BLOCKING': '#FF0000',
        'RUNNABLE': '#FFA500',
        'ASSIGNED': '#44A500',
	'COMMITTED': '#0000FF'
    };

    var linemap = {
	'RUNNABLE': { opacity: 0.4, stroke: '#009922', 'stroke-width': 3 },
	'ASSIGNED': { opacity: 0.9, stroke: '#335522', 'stroke-width': 4 },
	'COMMITTED': { opacity: 0.2, stroke: '#000000',	'stroke-width': 1 }
    };

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

    create_task = function(t) {

	taskmap[t.task_id] = t;
	t.display_index = last_index++;
	t.display_coords = display_index_to_coords(t.display_index);
	t.circle = paper.circle(t.display_coords.x, t.display_coords.y, radius);
	t.circle.attr({
		'fill': colmap[t.state],
		'stroke-width': 1
		    });       

	t.outgoing_lines = [];
	if (with_lines) {
	    for (var x in t.deps) {
		var tcoords = taskmap[t.deps[x]].display_coords;
		var l = paper.path("M" + t.display_coords.x + " " + t.display_coords.y + "L" + tcoords.x + " " + tcoords.y);
		t.outgoing_lines.push({
			to: t.deps[x],
			line: l
			    });
		l.attr('opacity', '0.1');
		l.toBack();
	    }
	}

    }

    for (var t in json.taskmap) {

	create_task(json.taskmap[t]);

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
		    create_task(ev.task_descriptor);
		}
	    }
	    else {

		var taskd = taskmap[ev.task_id];
		
		if(taskd.event_index < ev.index) {
		    taskd.circle.animate({'fill': colmap[ev.action]}, 200);
		    for (x in taskd.outgoing_lines) {
			taskd.outgoing_lines[x].animate(linemap[ev.action], 200);
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
