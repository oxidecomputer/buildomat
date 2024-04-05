var EVT;

var STREAM_TO_COLOUR = {
	"stdout": "#ffffff",
	"stderr": "#ffd9da",
	"task": "#add8e6",
	"worker": "#fafad2",
	"control": "#90ee90",
	"console": "#e7d1ff",
};

function
live_onload()
{
	EVT = new EventSource("./%CHECKRUN%/live");
	EVT.addEventListener("check", live_check);
	EVT.addEventListener("job", live_job);
	EVT.addEventListener("complete", live_complete);
}

function
live_check(ev)
{
	console.log("check received; ending stream");
	EVT.close();
}

function
live_job(ev)
{
	var je;

	try {
		je = JSON.parse(ev.data);
	} catch (ex) {
		console.error("parse error: " + ex);
		return;
	}

	/*
	 * Locate the output table so that we can append a row to it.
	 */
	let tbl = document.getElementById("table_output");
	if (!tbl) {
		return;
	}

	/*
	 * Determine the row colour based on the stream to which this event
	 * belongs.
	 */
	var colour = "#dddddd";
	if (je.stream.startsWith("bg.")) {
		colour = "#f79d65";
	} else if (STREAM_TO_COLOUR[je.stream]) {
		colour = STREAM_TO_COLOUR[je.stream];
	}

	var tr = document.createElement("tr");
	tr.style.cssText = "background-color: " + colour + ";";

	var td0 = document.createElement("td");
	td0.style.cssText = "vertical-align: top; text-align: right;";
	var span0 = document.createElement("span");
	span0.style.cssText = "white-space: pre; font-family: monospace;";
	span0.innerText = "" + je.seq;
	td0.appendChild(span0);
	tr.appendChild(td0);

	var td1 = document.createElement("td");
	td1.style.cssText = "vertical-align: top;";
	var span1 = document.createElement("span");
	span1.style.cssText = "white-space: pre; font-family: monospace;";
	span1.innerText = "" + new Date(je.time).toISOString();
	td1.appendChild(span1);
	tr.appendChild(td1);

	var td2 = document.createElement("td");
	td2.style.cssText = "vertical-align: top;";
	var span2 = document.createElement("span");
	span2.style.cssText = "white-space: pre-wrap; " +
	    "white-space: -moz-pre-wrap !important; " +
	    "font-family: monospace;";
	span2.innerText = "" + je.payload;
	td2.appendChild(span2);
	tr.appendChild(td2);

	tbl.appendChild(tr);

	tr.scrollIntoView(false);
}

function
live_complete(ev)
{
	console.log("complete received; ending stream");
	EVT.close();
}
