var EVT;
var LOCAL_TIME = %LOCAL_TIME%;

function
basic_onload()
{
	EVT = new EventSource("./%CHECKRUN%/live?minseq=%MINSEQ%");
	EVT.addEventListener("check", basic_check);
	EVT.addEventListener("job", basic_job);
	EVT.addEventListener("complete", basic_complete);
}

function
basic_check(ev)
{
	console.log("check received; ending stream");
	EVT.close();
}

function
basic_job(ev)
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

	var tr = document.createElement("tr");
	tr.className = je[0];

	for (var i = 1; i < je.length; i++) {
		var td = document.createElement("td");
		if (i === 1) {
			td.className = "num";
		}

		var span = document.createElement("span");
		if (i === 4) {
			td.className = "payload";
		} else {
			td.className = "field";
		}
		span.innerText = t[i];
		td.appendChild(span);
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
