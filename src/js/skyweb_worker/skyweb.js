/* Skylight visualisation for Skywriting event streams.
   (c) 2010, Anil Madhavapeddy <anil@recoil.org>
   Adapted for live viewing by Chris Smowton <chris@smowton.net>
*/

Skyweb = function(json) {

    var log_table = $("#skyweb-worker-log");
    var latest_log_entry = 0;

    notify_events_callback = function(json) {
	
	if(json.error) {
	    alert("Failed to await log entries: The server says \"" + json.error + "\". Refresh to try again.");
	}
	else {
	    $.getJSON("../log/?first_event=" + latest_log_entry + "&last_event=" + (latest_log_entry + 1000), process_events_callback);
	}

    };

    process_events_callback = function(json) {

	html_of_event = function(event) {

	    return "<td class=\"worker-log-time\">" + event.time + "</td><td class=\"worker-log-evemt\">" + event.event + "</td>";
	    
	}

	for(var i in json) {

	    latest_log_entry++;
	    new_row = $("<tr>" + html_of_event(json[i]) + "</tr>")
	    log_table.append(new_row);

	}

	$.post("../log/wait_after", {event_count: latest_log_entry}, notify_events_callback, "json");

    };

};
