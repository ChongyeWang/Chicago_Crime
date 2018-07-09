// *** This page uses angular as the way it interacts with the user input
angular.module("app", [])
    .controller("ctrl", function($scope) {
        var parseTime = d3.timeParse("%Y-%m-%dT%H:%M:%S");
        // Get the data
        people = ["All"]
        d3.csv("viz_data/dummyHR.csv", function (error, data) {
            //console.log(data)
            var count = 0
            data.forEach(function (d) {
                d.timestamp = parseTime(d.timestamp);
                //console.log(count, d.timestamp)
                count = count + 1
                if(!people.includes(d.person)){
                    people.push(d.person)
                }
            });

            for(var person in people){
                people[person] = { "text" : people[person] }
            }
            // Select X-axis Variable
            var span = d3.select("#chart").append('span')
                .text('Select Person: ')
            var yInput = d3.select("#chart").append('select')
                .attr('id','ySelect')
                .on('change',yChange)
                .selectAll('option')
                .data(people)
                .enter()
                .append('option')
                .attr('value', function (d) { return d.text })
                .text(function (d) { return d.text ;})
            d3.select("#chart").append('br')

            // *** Set up svg
            var margin = {
                    top: 30,
                    right: 20,
                    bottom: 30,
                    left: 50
                },
                width = 960 - margin.left - margin.right,
                height = 500 - margin.top - margin.bottom;


            var tooltip = d3.select("#chart").append("div")
                .attr("class", "tooltip")
                .style("opacity", 0);

            var x = d3.scaleTime()
                .range([0, width])
                .nice();

            var y = d3.scaleLinear().range([height, 0]);

            x.domain(d3.extent(data, function (d) {
                return d.timestamp;
            })).nice();
            y.domain(d3.extent(data, function (d) {
                return d.value;
            })).nice();

            var xAxis = d3.axisBottom(x).tickFormat(d3.timeFormat("%H:%M"));
            var yAxis = d3.axisLeft(y);


            var brush = d3.brush().extent([[0, 0], [width, height]]).on("end", brushended),
                idleTimeout,
                idleDelay = 350;

            var zoom = d3.zoom().extent([[0, 0], [width, height]]).on("zoom", zoomed);

            var chart_svg = d3.select("#chart").append("svg")
                .attr("width", width + margin.left + margin.right)
                .attr("height", height + margin.top + margin.bottom);


            var svg = chart_svg.append("g")
                .attr("transform", "translate(" + margin.left + "," + margin.top + ")");

            zoom(svg);

            svg.append("text")
                .attr("x", (width / 2))
                .attr("y", 0 - (margin.top / 2))
                .attr("text-anchor", "middle")
                .style("font-size", "16px")
                .style("text-decoration", "underline");

            var clip = svg.append("defs").append("svg:clipPath")
                .attr("id", "clip")
                .append("svg:rect")
                .attr("width", width)
                .attr("height", height)
                .attr("x", 0)
                .attr("y", 0);

            var scatter = svg.append("g")
                .attr("id", "scatterplot")
                .attr("clip-path", "url(#clip)");


            var cValue = function (d) {
                    return d.person;
                },
                color = d3.scaleOrdinal(d3.schemeCategory10);


            // x axis
            svg.append("g")
                .attr("class", "x axis")
                .attr('id', "axis--x")
                .attr("transform", "translate(0," + height + ")")
                .call(xAxis);
            //.selectAll("text")
            //.style("text-anchor", "end")
            //.attr("dx", "-.8em")
            //.attr("dy", ".15em");
            //.attr("transform", "rotate(-65)");


            svg.append("text")
                .style("text-anchor", "end")
                .attr("x", width)
                .attr("y", height - 8)
                .text("Time");

            // y axis
            svg.append("g")
                .attr("class", "y axis")
                .attr('id', "axis--y")
                .call(yAxis);

            svg.append("text")
                .attr("transform", "rotate(-90)")
                .attr("y", 6)
                .attr("dy", "1em")
                .style("text-anchor", "end")
                .text("Value");

            scatter.append("g")
                .attr("class", "brush")
                .call(brush);


            $scope.reset = function () {
                x.domain(d3.extent(data, function (d) {
                    return d.timestamp;
                })).nice();
                y.domain(d3.extent(data, function (d) {
                    return d.value;
                })).nice();
                zooming();
            };

            $scope.updateFilter = function () {

                var pivotKeys = d3.map(data, function (d) {
                    return d.value;
                }).keys();
                var filteredPivotArr = pivotKeys.filter(function (dataItem) {
                    var re = new RegExp($scope.filteringSelection, "i");
                    var result = dataItem.match(re);

                    if (result != null) {
                        return true;
                    } else {
                        return false;
                    }

                });


                var filteredPivotObj = [];

                for (var k in data) {

                    if (filteredPivotArr.includes(data[k].pivot)) {

                        filteredPivotObj.push(data[k]);
                    }
                }

                plotData(filteredPivotObj);
            };

            plotData(data);
            function plotData(data) {

                scatter.selectAll(".dot").remove();
                scatter.selectAll(".dot")
                    .data(data)
                    .enter().append("circle")
                    .attr("class", "dot")
                    .attr("r", 4)
                    .attr("cx", function (d) {
                        return x(d.timestamp);
                    })
                    .attr("cy", function (d) {
                        return y(d.value);
                    })
                    .attr("opacity", 0.5)
                    .style("fill", function (d) {
                        return color(cValue(d));
                    })
                    .on("mouseover", function (d) {
                        tooltip.transition()
                            .duration(200)
                            .style("opacity", .9);
                        tooltip.html(d.person + "-" + d.value + " ,Timestamp: " + d.timestamp)
                            .style("left", (d3.event.pageX) + "px")
                            .style("top", (d3.event.pageY) + "px");

                    })
                    .on("mouseout", function (d) {
                        tooltip.transition()
                            .duration(500)
                            .style("opacity", 0);
                    });

            }

            // Get a subset of the data based on the group
            function getFilteredData(data, group) {
                console.log(group)
                return data.filter(function(point) { return point.person == group; });
            }

            function yChange() {
                var groupdata = data
                if(this.value != "All")
                    groupdata = getFilteredData(data,  this.value);
                console.log(groupdata)
                plotData(groupdata);
            }


            function brushended() {
                var s = d3.event.selection;
                console.log(s)
                if (!s) {
                    if (!idleTimeout) return idleTimeout = setTimeout(idled, idleDelay);
                } else {
                    x.domain([s[0][0], s[1][0]].map(x.invert, x));
                    y.domain([s[1][1], s[0][1]].map(y.invert, y));
                    scatter.select(".brush").call(brush.move, null);
                }
                zooming();
            }

            function idled() {
                idleTimeout = null;
            }

            function zooming() {
                var t = scatter.transition().duration(750);
                svg.select("#axis--x").transition(t).call(xAxis).selectAll("text");
                //.style("text-anchor", "end")
                //.attr("dx", "-.8em")
                //.attr("dy", ".15em");
                //.attr("transform", "rotate(-65)");

                svg.select("#axis--y").transition(t).call(yAxis);
                scatter.selectAll("circle").transition(t)
                    .attr("cx", function (d) {
                        return x(d.timestamp);
                    });
                //.attr("cy", function (d) { return y(d.pivot); });
            }

            function zoomed() {
                if (d3.event.sourceEvent && d3.event.sourceEvent.type === "brush") return; // ignore zoom-by-brush
                var t = d3.event.transform;
                x.domain(t.rescaleX(x).domain());
                svg.select("#axis--x").call(xAxis);
                scatter.select(".brush").call(brush.move, x.range().map(t.invertX, t));
            }

            var legend = svg.selectAll(".legend")
                .data(color.domain())
                .enter().append("g")
                .classed("legend", true)
                .attr("transform", function(d, i) { return "translate(0," + i * 20 + ")"; });

            legend.append("circle")
                .attr("r", 3.5)
                .attr("cx", width + 20)
                .attr("fill", color);

            legend.append("text")
                .attr("x", width + 26)
                .attr("dy", ".35em")
                .text(function(d) { return d; });

        });
    });