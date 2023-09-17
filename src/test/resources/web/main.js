//https://tabulator.info/docs/5.5/quickstart#sources-bower

const URL_COUNT = "../api/count";
const URL_SEARCH = "../api/search";
const URL_REENQUEUE = "../api/reenqueue";
const URL_MARK_POISON = "../api/markpoison";

async function showCountTable() {
    const resp = await fetch(URL_COUNT);
    const jsonData = await resp.json();
    const table = await new Tabulator("#table-count", {
        data: jsonData,
        layout: "fitDataStretch",
        //placeholder: "No Data Set",
        columns: [
            { title: "queueName", field: "queueName" },
            { title: "status", field: "status" },
            { title: "count", field: "count", sorter: "number" },
        ],
    });
    return table;
}

async function showListTable() {
    const jsonData = [];
    //const jsonData = [{"id":1162,"referenceNumber":"REF_1162","queueName":"payment.check","status":"PROCESSING","attempt":1,"createTime":"2023-09-16T20:41:12.462084+08:00","nextProcessTime":"2023-09-16T20:51:20.229127+08:00","lastUpdateTime":"2023-09-16T20:41:20.229478+08:00","payload":"{\"id\":1162,\"status\":\"CREATED\",\"createTime\":\"2023-09-16T20:41:12.417951208+08:00\"}"},{"id":1164,"referenceNumber":"REF_1163","queueName":"payment.check","status":"PROCESSING","attempt":1,"createTime":"2023-09-16T20:41:12.462554+08:00","nextProcessTime":"2023-09-16T20:51:20.229127+08:00","lastUpdateTime":"2023-09-16T20:41:20.229478+08:00","payload":"{\"id\":1163,\"status\":\"CREATED\",\"createTime\":\"2023-09-16T20:41:12.418005693+08:00\"}"},{"id":1163,"referenceNumber":"REF_1164","queueName":"payment.check","status":"PROCESSING","attempt":1,"createTime":"2023-09-16T20:41:12.462559+08:00","nextProcessTime":"2023-09-16T20:51:20.229127+08:00","lastUpdateTime":"2023-09-16T20:41:20.229478+08:00","payload":"{\"id\":1164,\"status\":\"CREATED\",\"createTime\":\"2023-09-16T20:41:12.418018817+08:00\"}"},{"id":1165,"referenceNumber":"REF_1165","queueName":"payment.check","status":"PROCESSING","attempt":1,"createTime":"2023-09-16T20:41:12.462558+08:00","nextProcessTime":"2023-09-16T20:51:20.229127+08:00","lastUpdateTime":"2023-09-16T20:41:20.229478+08:00","payload":"{\"id\":1165,\"status\":\"CREATED\",\"createTime\":\"2023-09-16T20:41:12.417978614+08:00\"}"},{"id":1166,"referenceNumber":"REF_1166","queueName":"payment.check","status":"PROCESSING","attempt":1,"createTime":"2023-09-16T20:41:12.462928+08:00","nextProcessTime":"2023-09-16T20:51:20.229127+08:00","lastUpdateTime":"2023-09-16T20:41:20.229478+08:00","payload":"{\"id\":1166,\"status\":\"CREATED\",\"createTime\":\"2023-09-16T20:41:12.418045994+08:00\"}"},{"id":1167,"referenceNumber":"REF_1167","queueName":"payment.check","status":"PROCESSING","attempt":1,"createTime":"2023-09-16T20:41:12.46301+08:00","nextProcessTime":"2023-09-16T20:51:20.273352+08:00","lastUpdateTime":"2023-09-16T20:41:20.274311+08:00","payload":"{\"id\":1167,\"status\":\"CREATED\",\"createTime\":\"2023-09-16T20:41:12.418032301+08:00\"}"},{"id":1168,"referenceNumber":"REF_1170","queueName":"payment.check","status":"PROCESSING","attempt":1,"createTime":"2023-09-16T20:41:12.463242+08:00","nextProcessTime":"2023-09-16T20:51:20.273352+08:00","lastUpdateTime":"2023-09-16T20:41:20.274311+08:00","payload":"{\"id\":1170,\"status\":\"CREATED\",\"createTime\":\"2023-09-16T20:41:12.41809145+08:00\"}"},{"id":1169,"referenceNumber":"REF_1168","queueName":"payment.check","status":"PROCESSING","attempt":1,"createTime":"2023-09-16T20:41:12.463242+08:00","nextProcessTime":"2023-09-16T20:51:20.273352+08:00","lastUpdateTime":"2023-09-16T20:41:20.274311+08:00","payload":"{\"id\":1168,\"status\":\"CREATED\",\"createTime\":\"2023-09-16T20:41:12.418058758+08:00\"}"},{"id":1170,"referenceNumber":"REF_1171","queueName":"payment.check","status":"PROCESSING","attempt":1,"createTime":"2023-09-16T20:41:12.463257+08:00","nextProcessTime":"2023-09-16T20:51:20.273352+08:00","lastUpdateTime":"2023-09-16T20:41:20.274311+08:00","payload":"{\"id\":1171,\"status\":\"CREATED\",\"createTime\":\"2023-09-16T20:41:12.418072542+08:00\"}"},{"id":1171,"referenceNumber":"REF_1169","queueName":"payment.check","status":"PROCESSING","attempt":1,"createTime":"2023-09-16T20:41:12.463406+08:00","nextProcessTime":"2023-09-16T20:51:20.273352+08:00","lastUpdateTime":"2023-09-16T20:41:20.274311+08:00","payload":"{\"id\":1169,\"status\":\"CREATED\",\"createTime\":\"2023-09-16T20:41:12.417965205+08:00\"}"}];
    const table = await new Tabulator("#table-list", {
        data: jsonData,
        //height: "500px",
        layout: "fitDataStretch",
        pagination: "local",
        paginationSize: 100,
        //placeholder: "No Data Set",
        columns: [
            {
                formatter: "rowSelection", titleFormatter: "rowSelection", hozAlign: "left", headerSort: false, cellClick: function (e, cell) {
                    cell.getRow().toggleSelect();
                }
            },
            { title: "id", field: "id", sorter: "number" },
            { title: "referenceNumber", field: "referenceNumber" },
            { title: "status", field: "status" },
            { title: "attempt", field: "attempt", hozAlign: "right", sorter: "number" },
            { title: "createTime", field: "createTime" },
            { title: "nextProcessTime", field: "nextProcessTime" },
            { title: "lastUpdateTime", field: "lastUpdateTime" },
            { title: "payload", field: "payload"},
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

countTable.on("rowClick", function (e, row) {
    const data = row.getData();
    console.log("selected:", data);
    listTable.replaceData(URL_SEARCH + "/" + data.queueName + "/" + data.status);
});

document.getElementById("btn-refresh").onclick = function(e) {
     countTable.replaceData(URL_COUNT);
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

// for browser debug
window.countTable = countTable;
window.listTable = listTable;
