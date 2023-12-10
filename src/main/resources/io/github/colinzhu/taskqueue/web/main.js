//https://tabulator.info/docs/5.5/quickstart#sources-bower

const URL_COUNT = "../api/count";
const URL_SEARCH = "../api/search";
const URL_REENQUEUE = "../api/reenqueue";
const URL_MARK_POISON = "../api/mark-poison";
const URL_POISON_TO_ERROR = "../api/poison-to-error";
const URL_HOUSEKEEP_POISON = "../api/housekeep-poison";

async function fetchAndTransformCountData(url) {
    const resp = await fetch(url);
    const jsonData = await resp.json();

    // Transform the data
    const transformedData = {};
    jsonData.forEach(({ queueName, status, count }) => {
        if (!transformedData[queueName]) {
            transformedData[queueName] = {};
            transformedData[queueName]['TOTAL'] = 0;
        }
        transformedData[queueName][status] = count;
        transformedData[queueName]['TOTAL'] += count;
    });

    // Convert the transformed data back to an array
    const data = Object.entries(transformedData).map(([queueName, counts]) => ({
        queueName,
        ...counts
    }));

    return data;
}

async function showCountTable() {
    const data = await fetchAndTransformCountData(URL_COUNT);

    // Define the columns
    const columns = [
        { title: "QUEUE", field: "queueName" },
        { title: "CREATED", field: "CREATED" },
        { title: "PROCESSING", field: "PROCESSING" },
        { title: "COMPLETED", field: "COMPLETED" },
        { title: "ERROR", field: "ERROR" },
        { title: "POISON", field: "POISON" },
        { title: "TOTAL", field: "TOTAL" }
    ];

    const table = await new Tabulator("#table-count", {
        data,
        layout: "fitDataStretch",
        columns
    });

    return table;
}

async function fetchDataAndUpdateCountTable(table, url) {
    const data = await fetchAndTransformCountData(url);
    table.replaceData(data);
}

async function showListTable() {
    const jsonData = [];
    //const jsonData = [{"id":1162,"referenceNumber":"REF_1162","queueName":"payment.check","status":"PROCESSING","attempt":1,"createTime":"2023-09-16T20:41:12.462084+08:00","nextProcessTime":"2023-09-16T20:51:20.229127+08:00","lastUpdateTime":"2023-09-16T20:41:20.229478+08:00","payload":"{\"id\":1162,\"status\":\"CREATED\",\"createTime\":\"2023-09-16T20:41:12.417951208+08:00\"}"},{"id":1164,"referenceNumber":"REF_1163","queueName":"payment.check","status":"PROCESSING","attempt":1,"createTime":"2023-09-16T20:41:12.462554+08:00","nextProcessTime":"2023-09-16T20:51:20.229127+08:00","lastUpdateTime":"2023-09-16T20:41:20.229478+08:00","payload":"{\"id\":1163,\"status\":\"CREATED\",\"createTime\":\"2023-09-16T20:41:12.418005693+08:00\"}"},{"id":1163,"referenceNumber":"REF_1164","queueName":"payment.check","status":"PROCESSING","attempt":1,"createTime":"2023-09-16T20:41:12.462559+08:00","nextProcessTime":"2023-09-16T20:51:20.229127+08:00","lastUpdateTime":"2023-09-16T20:41:20.229478+08:00","payload":"{\"id\":1164,\"status\":\"CREATED\",\"createTime\":\"2023-09-16T20:41:12.418018817+08:00\"}"},{"id":1165,"referenceNumber":"REF_1165","queueName":"payment.check","status":"PROCESSING","attempt":1,"createTime":"2023-09-16T20:41:12.462558+08:00","nextProcessTime":"2023-09-16T20:51:20.229127+08:00","lastUpdateTime":"2023-09-16T20:41:20.229478+08:00","payload":"{\"id\":1165,\"status\":\"CREATED\",\"createTime\":\"2023-09-16T20:41:12.417978614+08:00\"}"},{"id":1166,"referenceNumber":"REF_1166","queueName":"payment.check","status":"PROCESSING","attempt":1,"createTime":"2023-09-16T20:41:12.462928+08:00","nextProcessTime":"2023-09-16T20:51:20.229127+08:00","lastUpdateTime":"2023-09-16T20:41:20.229478+08:00","payload":"{\"id\":1166,\"status\":\"CREATED\",\"createTime\":\"2023-09-16T20:41:12.418045994+08:00\"}"},{"id":1167,"referenceNumber":"REF_1167","queueName":"payment.check","status":"PROCESSING","attempt":1,"createTime":"2023-09-16T20:41:12.46301+08:00","nextProcessTime":"2023-09-16T20:51:20.273352+08:00","lastUpdateTime":"2023-09-16T20:41:20.274311+08:00","payload":"{\"id\":1167,\"status\":\"CREATED\",\"createTime\":\"2023-09-16T20:41:12.418032301+08:00\"}"},{"id":1168,"referenceNumber":"REF_1170","queueName":"payment.check","status":"PROCESSING","attempt":1,"createTime":"2023-09-16T20:41:12.463242+08:00","nextProcessTime":"2023-09-16T20:51:20.273352+08:00","lastUpdateTime":"2023-09-16T20:41:20.274311+08:00","payload":"{\"id\":1170,\"status\":\"CREATED\",\"createTime\":\"2023-09-16T20:41:12.41809145+08:00\"}"},{"id":1169,"referenceNumber":"REF_1168","queueName":"payment.check","status":"PROCESSING","attempt":1,"createTime":"2023-09-16T20:41:12.463242+08:00","nextProcessTime":"2023-09-16T20:51:20.273352+08:00","lastUpdateTime":"2023-09-16T20:41:20.274311+08:00","payload":"{\"id\":1168,\"status\":\"CREATED\",\"createTime\":\"2023-09-16T20:41:12.418058758+08:00\"}"},{"id":1170,"referenceNumber":"REF_1171","queueName":"payment.check","status":"PROCESSING","attempt":1,"createTime":"2023-09-16T20:41:12.463257+08:00","nextProcessTime":"2023-09-16T20:51:20.273352+08:00","lastUpdateTime":"2023-09-16T20:41:20.274311+08:00","payload":"{\"id\":1171,\"status\":\"CREATED\",\"createTime\":\"2023-09-16T20:41:12.418072542+08:00\"}"},{"id":1171,"referenceNumber":"REF_1169","queueName":"payment.check","status":"PROCESSING","attempt":1,"createTime":"2023-09-16T20:41:12.463406+08:00","nextProcessTime":"2023-09-16T20:51:20.273352+08:00","lastUpdateTime":"2023-09-16T20:41:20.274311+08:00","payload":"{\"id\":1169,\"status\":\"CREATED\",\"createTime\":\"2023-09-16T20:41:12.417965205+08:00\"}"}];
    const table = await new Tabulator("#table-list", {
        data: jsonData,
        layout: "fitDataStretch",
        pagination: "local",
        //paginationElement:document.getElementById("custom-pagination"), 
        paginationSize: 10,
        paginationSizeSelector:[10, 50, 100, 200, 500, 1000],
        paginationCounter: "rows",
        columns: [
            {
                formatter: "rowSelection", titleFormatter: "rowSelection", titleFormatterParams: {
                    rowRange: "visible" //only current page rows
                }, headerSort: false
            },
            { title: "id", field: "id", sorter: "number" },
            { title: "queueName", field: "queueName" },
            { title: "referenceNumber", field: "referenceNumber" },
            { title: "status", field: "status" },
            { title: "attempt", field: "attempt", hozAlign: "right", sorter: "number" },
            { title: "createTime", field: "createTime" },
            { title: "pollerInstance", field: "pollerInstance" },
            { title: "nextProcessTime", field: "nextProcessTime" },
            { title: "lastUpdateTime", field: "lastUpdateTime" },
            { title: "payload", field: "payload" },
            { title: "processResult", field: "processResult" },
        ],
    });
    return table;
}
async function processSelected(listTable, countTable, batchUpdateUrl) {
    const selectedData = listTable.getSelectedData();
    if (selectedData.length == 0) {
        console.log("nothing selected");
        return;
    }
    const idList = selectedData.map(row => row.id);
    if (!confirm("number of records to be processed:" + idList.length)) {
        return;
    }
    console.log("taskIdList:", idList);
    const resp = await fetch(batchUpdateUrl, {
        method: 'POST',
        body: JSON.stringify(idList),
        headers: {
            'Content-type': 'application/json; charset=UTF-8',
        }
    });
    if (!resp.ok) {
        alert("error:" + resp.status + " " + await resp.text());
        console.log(resp);
        return;
    }
    const jsonData = await resp.json()
    console.log("process response:", jsonData);
    alert("number of records processed:" + jsonData.count);

    // refresh list table
    listTable.replaceData(listTable.getAjaxUrl());
    countTable.replaceData(URL_COUNT);

}

const countTable = await showCountTable();
const listTable = await showListTable();

// when click the cell of countTable, refresh listTable
countTable.on("cellClick", function (e, cell) {
    const data = cell.getRow().getData();
    const field = cell.getField();
    console.log("selected:", data, field);
    if (field !== 'queueName' && field !== 'TOTAL') {
        listTable.replaceData(URL_SEARCH + "/" + data.queueName + "/" + field + "?size=1000");
    }
});

listTable.on("rowSelectionChanged", function (data, rows) {
    document.getElementById("select-stats").innerHTML = "Selected: " + data.length;
});

document.getElementById("btn-refresh").onclick = async function (e) {
    await fetchDataAndUpdateCountTable(countTable, URL_COUNT);
    const listTableAjaxUrl = listTable.getAjaxUrl();
    if (listTableAjaxUrl != null && listTableAjaxUrl != "") {
        listTable.replaceData(listTableAjaxUrl);
    }
}

document.getElementById("btn-reenqueue-selected").onclick = function (e) {
    processSelected(listTable, countTable, URL_REENQUEUE);
};

document.getElementById("btn-markpoison-selected").onclick = function (e) {
    processSelected(listTable, countTable, URL_MARK_POISON);
};

document.getElementById("btn-poisontoerror-selected").onclick = function (e) {
    processSelected(listTable, countTable, URL_POISON_TO_ERROR);
};

document.getElementById("btn-housekeeppoison-selected").onclick = function (e) {
    processSelected(listTable, countTable, URL_HOUSEKEEP_POISON);
};

// for browser debug
window.countTable = countTable;
window.listTable = listTable;
