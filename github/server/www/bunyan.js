function selectMaxLevel(select) {
    let minIdx = select.selectedIndex;
    let options = select.options;

    console.info("setting minimum shown level to" + options[minIdx]);

    for (let i = 0; i < options.length; i++) {
        let display = i >= minIdx ? 'table-row' : 'none';
        let levelClass = options[i].value;

        console.info("setting \'." + levelClass + "\' to \'display: " + 
            display + "\'");

        let logs = document.getElementsByClassName(levelClass);
        for (let log of logs) {
            log.style.display = display;
        }
    }
}
