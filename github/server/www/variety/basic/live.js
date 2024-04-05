var EVT;
var LOCAL_TIME = %LOCAL_TIME%;
var LAST_TASK = null;
var LAST_EVENT = null;

function
basic_onload()
{
	EVT = new EventSource("./%CHECKRUN%/live?minseq=%MINSEQ%");
	EVT.addEventListener("check", basic_check);
	EVT.addEventListener("row", basic_row);
	EVT.addEventListener("complete", basic_complete);
}

function
basic_check(ev)
{
	console.log("check received; ending stream");
	EVT.close();
}

function
basic_row(ev)
{
	var evr;

	/*
	 * For debugging purposes, stash the most recent row event we have
	 * received:
	 */
	LAST_EVENT = ev;

	try {
		/*
		 * The server is sending us an EventRow object:
		 */
		evr = JSON.parse(ev.data);
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
	 * If the task has changed, render a full-width blank row in the table:
	 */
	if (evr.task !== LAST_TASK) {
		let count = 0;
		for (let i = 0; i < evr.fields.length; i++) {
			let f = evr.fields[i];

			if (!f.local_time || LOCAL_TIME) {
				count++;
			}
		}

		let tr = document.createElement("tr");
		let td = document.createElement("td");
		td.colSpan = count.toString();
		td.innerHTML = "&nbsp";
		tr.appendChild(td);
		tbl.appendChild(td);

		LAST_TASK = evr.task;
	}

	let tr = document.createElement("tr");
	tr.className = evr.css_class;

	for (let i = 0; i < evr.fields.length; i++) {
		let f = evr.fields[i];

		if (f.local_time && !LOCAL_TIME) {
			continue;
		}

		let td = document.createElement("td");

		if (!!f.anchor) {
			td.className = f.css_class;

			let anc = document.createElement("a");
			anc.id = f.anchor;

			let lnk = document.createElement("a");
			lnk.className = f.css_class + "link";
			lnk.href = "#" + f.anchor;
			lnk.innerHTML = f.value;

			td.appendChild(anc);
			td.appendChild(lnk);
		} else {
			let spn = document.createElement("span");
			spn.className = f.css_class;
			spn.innerHTML = f.value;

			td.appendChild(spn);
		}

		tr.appendChild(td);
	}

	tbl.appendChild(tr);

	tr.scrollIntoView(false);
}

function
basic_complete(ev)
{
	console.log("complete received; ending stream");
	EVT.close();
}
