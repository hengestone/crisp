// save the chart instance to a variable
var fgColors = [
  "rgba(109, 135, 100, 1)", // green
  "rgba(100, 118, 135, 1)", // blue
  "rgba(118, 96, 138, 1)", // purple
  "#7EA6E0", // light blue

  "rgba(229, 20, 0, 1)", // red
  "rgba(250, 104, 0, 1)", // orange
  "rgba(240, 163, 10, 1)", // light orange
  "rgba(227, 200, 0, 1)" // yellow
];

var bgColors = [
  "rgb(58, 84, 49)",
  "rgb(49, 67, 84)",
  "rgb(67, 45, 87)",
  "#314354",
  "rgb(178, 0, 0)",
  "rgb(199, 53, 0)",
  "rgb(189, 112, 0)",
  "rgb(176, 149, 0)"
];
var ctx = document.getElementById("bar-chart");
var myChart = new Chart(ctx, {
  type: "bar",
  data: {
    labels: ["Queued", "Runnable", "Running", "Finished", "Failed"],
    datasets: [
      {
        label: "Number of Jobs",
        data: [0, 0, 0, 0, 0],
        barPercentage: 1,
        // categoryPercentage: 1,
        backgroundColor: fgColors,
        borderColor: bgColors,
        borderWidth: 1
      }
    ]
  },
  options: {
    plugins: {
      datalabels: {
        color: "white"
      }
    },
    scales: {
      yAxes: [
        {
          type: "logarithmic",
          ticks: {
            beginAtZero: true,
            max: 1000.0,
            min: 0.9
          }
        }
      ]
    }
  }
});

function ri(max) {
  return Math.floor(Math.random() * Math.floor(max));
}

function onReceive(name, new_counts) {
  myChart.data.datasets[0].data = new_counts;
  myChart.data.datasets[0].label = name;
  myChart.update({
    preservation: false
  });
  return myChart.data.datasets[0].data;
}
