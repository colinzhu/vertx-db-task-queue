//https://tabulator.info/docs/5.5/quickstart#sources-bower

async function showCountTable() {
    const resp = await fetch("../count");
    const jsonData = await resp.json();
    const table = await new Tabulator("#table-count", {
        data:jsonData,
        height:"120px",
        layout:"fitDataStretch",
        placeholder:"No Data Set",
        columns:[
            {title:"queueName", field:"queueName"},
            {title:"status", field:"status"},
            {title:"count", field:"count", sorter:"number"},
        ],
    });
    return table;
}

async function showListTable() {
    const jsonData = [];
    const table = await new Tabulator("#table-list", {
        data:jsonData,
        height:"500px",
        layout:"fitDataStretch",
        pagination:"local",
        paginationSize:100,
        placeholder:"No Data Set",
        columns:[
            {formatter:"rowSelection", titleFormatter:"rowSelection", hozAlign:"center", headerSort:false, cellClick:function(e, cell){
                cell.getRow().toggleSelect();
              }},
            {title:"id", field:"id", sorter:"number"},
            {title:"referenceNumber", field:"referenceNumber"},
            {title:"status", field:"status"},
            {title:"attempt", field:"attempt", hozAlign:"right", sorter:"number"},
            {title:"createTime", field:"createTime"},
            {title:"nextProcessTime", field:"nextProcessTime"},
            {title:"lastUpdateTime", field:"lastUpdateTime"},
            {title:"payload", field:"payload"},
        ],
    });
    return table;
}

const countTable = await showCountTable();
const listTable = await showListTable();

countTable.on("rowClick", function(e, row){
    const data = row.getData();
    console.log("selected: " + data);
    listTable.setData("../search/" + data.queueName + "/" + data.status);
});

document.getElementById("btn-count").onclick = e => tableCount.setData("../count");

//window.tableCount = tableCount;
