/*
 * Copyright 2024 Oxide Computer Company
 */

var EVT;
var LOCAL_TIME = %LOCAL_TIME%;
var LAST_TASK = null;
var LAST_EVENT = null;

/*
 * State tracking for the "new log record" button and auto-scroll mechanism:
 */
var NEW_RECORDS = false;
var LAST_NEW_RECORD = null;

function
basic_onload()
{
	EVT = new EventSource("./%CHECKRUN%/live?minseq=%MINSEQ%");
	EVT.addEventListener("check", basic_check);
	EVT.addEventListener("row", basic_row);
	EVT.addEventListener("complete", basic_complete);

	document.addEventListener("scroll", basic_scroll);
	basic_scroll();
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

	/*
	 * Stash the last row we've appended to the log table so that we can
	 * scroll down to it when the user clicks the button.
	 */
	LAST_NEW_RECORD = tr;

	if (at_base()) {
		/*
		 * If we're at the base already, continue to scroll down when
		 * new log records arrive.
		 */
		basic_show_new();
	} else {
		/*
		 * Otherwise, we want to draw the "new log record" button.
		 */
		NEW_RECORDS = true;
	}

	basic_new_records();
}

function
basic_complete(ev)
{
	console.log("complete received; ending stream");
	EVT.close();
}

/*
 * Scroll the viewport to the base of the page and update the new log records
 * button.
 */
function
basic_show_new()
{
	if (LAST_NEW_RECORD !== null) {
		LAST_NEW_RECORD.scrollIntoView(false);
	}

	basic_new_records();
}

/*
 * Attempt to guess whether the page is currently scrolled to the bottom or
 * not.
 */
function
at_base()
{
	var sy = window.scrollY;
	var ih = window.innerHeight;
	var bsh = document.body.scrollHeight;
	if (typeof (sy) !== "number" || typeof (ih) !== "number" ||
	    typeof (bsh) !== "number") {
		return (false);
	}

	return ((sy + ih) + 25 >= bsh);
}

/*
 * On-scroll event for the body of the page.
 */
function
basic_scroll()
{
	if (at_base()) {
		/*
		 * If the viewport is at the base of the page already, clear
		 * the new record indicator.
		 */
		NEW_RECORDS = false;
	}

	basic_new_records();
}

/*
 * Update the state of the "new log records" button on the screen.  This should
 * be called any time the viewport is moved, or new records are appended.
 */
function
basic_new_records()
{
	var nrb = document.getElementById("more-button");
	if (!nrb) {
		/*
		 * Draw the new records hovering button.
		 */
		nrb = document.createElement("div");
		nrb.id = "more-button";
		nrb.className = "more";
		nrb.hidden = true;

		let nrbt0 = document.createElement("span");
		nrbt0.className = "more_arrow";
		nrbt0.innerHTML = "⇊";

		let nrbt1 = document.createElement("span");
		nrbt1.innerHTML = " new log records ";

		let nrbt2 = document.createElement("span");
		nrbt2.className = "more_arrow";
		nrbt2.innerHTML = "⇊";

		nrb.appendChild(nrbt0);
		nrb.appendChild(nrbt1);
		nrb.appendChild(nrbt2);

		/*
		 * If the user clicks anywhere on the button, scroll them down
		 * to the base of the page.
		 */
		nrb.addEventListener("click", basic_show_new);

		document.body.appendChild(nrb);
	}

	if (at_base() || !NEW_RECORDS) {
		/*
		 * If we're at the base of the page, we'll scroll live for new
		 * records that show up and the button should disappear. We
		 * also shouldn't show the button if there are, as yet, no new
		 * records.
		 */
		nrb.hidden = true;
	} else {
		nrb.hidden = false;
	}
}
