(function () {
  var deferredInstallPrompt = null;
  var nav = document.querySelector("[data-nav]");
  var navToggle = document.querySelector("[data-nav-toggle]");
  var navBackdrop = document.querySelector("[data-nav-backdrop]");
  var installButton = document.querySelector("[data-install-button]");
  var compactNavQuery = window.matchMedia("(max-width: 1080px)");
  var chartMode = "area";
  var chartEntries = [];

  function isCompactNav() {
    return compactNavQuery.matches;
  }

  function setNavOpen(isOpen) {
    if (!nav || !navToggle) {
      return;
    }
    var shouldOpen = isCompactNav() && isOpen;
    nav.classList.toggle("is-open", shouldOpen);
    navToggle.classList.toggle("is-active", shouldOpen);
    navToggle.setAttribute("aria-expanded", shouldOpen ? "true" : "false");
    document.body.classList.toggle("nav-open", shouldOpen);
    if (navBackdrop) {
      navBackdrop.hidden = !shouldOpen;
    }
  }

  function syncNavState() {
    if (!nav || !navToggle) {
      return;
    }
    if (!isCompactNav()) {
      nav.classList.remove("is-open");
      navToggle.classList.remove("is-active");
      navToggle.setAttribute("aria-expanded", "false");
      document.body.classList.remove("nav-open");
      if (navBackdrop) {
        navBackdrop.hidden = true;
      }
    }
  }

  function formatNumber(value, decimals) {
    return Number(value || 0).toLocaleString("es-AR", {
      minimumFractionDigits: decimals,
      maximumFractionDigits: decimals
    });
  }

  function formatValue(value, kind) {
    if (kind === "money") {
      return "$" + formatNumber(value, 2);
    }
    if (kind === "money_rate") {
      return "$" + formatNumber(value, 2) + "/kWh";
    }
    return formatNumber(value, 3).replace(/,?0+$/, "").replace(/\.$/, "") + " kWh";
  }

  function formatAxisTick(value, kind) {
    if (kind === "money_rate") {
      return "$" + formatNumber(value, 2);
    }

    if (Math.abs(value) >= 1000) {
      var compact = formatNumber(value / 1000, 1).replace(/,?0+$/, "").replace(/\.$/, "") + "k";
      return kind === "money" ? "$" + compact : compact;
    }

    var rounded = formatNumber(value, kind === "money" ? 0 : 0);
    return kind === "money" ? "$" + rounded : rounded;
  }

  function buildSeries(config, mode) {
    var isMobile = window.innerWidth <= 720;
    return config.datasets.map(function (dataset) {
      var common = {
        name: dataset.label,
        data: dataset.values,
        connectNulls: false,
        emphasis: { focus: "series" }
      };

      if (mode === "bar") {
        return Object.assign({}, common, {
          type: "bar",
          barMaxWidth: isMobile ? 18 : 28,
          itemStyle: {
            color: dataset.color,
            borderRadius: [8, 8, 0, 0]
          }
        });
      }

      return Object.assign({}, common, {
        type: "line",
        smooth: false,
        showSymbol: !isMobile || config.labels.length <= 8,
        symbol: "circle",
        symbolSize: isMobile ? 5 : 7,
        lineStyle: {
          width: isMobile ? 2 : 2.5,
          color: dataset.color
        },
        itemStyle: {
          color: dataset.color
        },
        areaStyle: {
          color: dataset.fill
        }
      });
    });
  }

  function buildChartOption(config, mode) {
    var darkArea = mode === "area";
    var isMobile = window.innerWidth <= 720;
    var axisColor = darkArea ? "rgba(255,255,255,0.72)" : "#59676b";
    var lineColor = darkArea ? "rgba(255,255,255,0.10)" : "rgba(31,42,46,0.10)";

    return {
      animationDuration: 350,
      backgroundColor: darkArea ? "#1d1f22" : "#ffffff",
      textStyle: {
        fontFamily: "Manrope, sans-serif"
      },
      grid: {
        left: isMobile ? 48 : 64,
        right: isMobile ? 12 : 22,
        top: isMobile ? 22 : 28,
        bottom: isMobile ? 70 : 60
      },
      tooltip: {
        trigger: "axis",
        confine: true,
        backgroundColor: darkArea ? "rgba(29,31,34,0.96)" : "rgba(255,255,255,0.96)",
        borderWidth: 0,
        textStyle: {
          color: darkArea ? "#f8fafc" : "#1f2a2e"
        },
        formatter: function (params) {
          if (!params.length) {
            return "";
          }

          var labelIndex = params[0].dataIndex;
          var lines = [config.full_labels[labelIndex] || config.labels[labelIndex]];
          params.forEach(function (param) {
            if (param.data == null) {
              return;
            }
            lines.push(param.seriesName + ": " + formatValue(param.data, config.value_kind));
          });
          return lines.join("<br>");
        }
      },
      legend: {
        show: false
      },
      xAxis: {
        type: "category",
        data: config.labels,
        axisLine: {
          lineStyle: { color: darkArea ? "rgba(255,255,255,0.14)" : "rgba(31,42,46,0.16)" }
        },
        axisTick: { show: false },
        axisLabel: {
          color: axisColor,
          fontSize: isMobile ? 10 : 11,
          margin: isMobile ? 10 : 12,
          interval: "auto",
          hideOverlap: true,
          rotate: isMobile ? 0 : (config.labels.length > 8 ? 12 : 0)
        }
      },
      yAxis: {
        type: "value",
        min: config.min_value,
        max: config.max_value,
        splitNumber: 5,
        axisLine: { show: false },
        axisTick: { show: false },
        axisLabel: {
          color: axisColor,
          fontSize: isMobile ? 10 : 11,
          formatter: function (value) {
            return formatAxisTick(value, config.value_kind);
          }
        },
        splitLine: {
          lineStyle: { color: lineColor }
        }
      },
      series: buildSeries(config, mode)
    };
  }

  function resolveChartHeight() {
    if (window.innerWidth <= 720) {
      return 420;
    }
    if (window.innerWidth <= 1080) {
      return 400;
    }
    return 460;
  }

  function applyChartSize(entry) {
    entry.root.style.width = "100%";
    entry.root.style.height = resolveChartHeight() + "px";
  }

  function renderCharts() {
    chartEntries.forEach(function (entry) {
      applyChartSize(entry);
      entry.chart.resize();
      entry.chart.setOption(buildChartOption(entry.config, chartMode), true);
    });
  }

  function initializeCharts() {
    if (!window.echarts) {
      return;
    }

    document.querySelectorAll("[data-echart]").forEach(function (root) {
      var configScriptId = root.getAttribute("data-chart-config-id");
      var configScript = configScriptId ? document.getElementById(configScriptId) : null;
      if (!configScript) {
        return;
      }

      var config = JSON.parse(configScript.textContent);
      if (config.default_mode) {
        chartMode = config.default_mode;
      }

      var entry = {
        root: root,
        config: config,
        chart: null
      };

      applyChartSize(entry);
      entry.chart = window.echarts.init(root, null, { renderer: "canvas" });
      chartEntries.push(entry);
    });

    if (chartEntries.length) {
      renderCharts();
    }
  }

  if (navToggle) {
    navToggle.addEventListener("click", function () {
      if (!isCompactNav()) {
        return;
      }
      setNavOpen(!nav.classList.contains("is-open"));
    });
  }

  if (navBackdrop) {
    navBackdrop.addEventListener("click", function () {
      setNavOpen(false);
    });
  }

  document.addEventListener("click", function (event) {
    var chartButton = event.target.closest("[data-chart-mode-button]");
    if (chartButton) {
      var collection = chartButton.closest("[data-chart-collection]");
      if (!collection) {
        return;
      }

      chartMode = chartButton.getAttribute("data-chart-mode-button") || "area";
      collection.querySelectorAll("[data-chart-mode-button]").forEach(function (item) {
        var isActive = item === chartButton;
        item.classList.toggle("is-active", isActive);
        item.setAttribute("aria-pressed", isActive ? "true" : "false");
      });
      renderCharts();
      return;
    }

    if (isCompactNav() && event.target.closest(".sidebar a")) {
      setNavOpen(false);
      return;
    }

    if (
      isCompactNav() &&
      nav &&
      nav.classList.contains("is-open") &&
      !event.target.closest(".sidebar") &&
      !event.target.closest("[data-nav-toggle]")
    ) {
      setNavOpen(false);
    }
  });

  document.addEventListener("keydown", function (event) {
    if (event.key === "Escape") {
      setNavOpen(false);
    }
  });

  window.addEventListener("resize", function () {
    syncNavState();
    renderCharts();
  });

  syncNavState();

  window.addEventListener("beforeinstallprompt", function (event) {
    event.preventDefault();
    deferredInstallPrompt = event;
    if (installButton) {
      installButton.hidden = false;
    }
  });

  window.addEventListener("appinstalled", function () {
    deferredInstallPrompt = null;
    if (installButton) {
      installButton.hidden = true;
    }
  });

  if (installButton) {
    installButton.addEventListener("click", async function () {
      if (!deferredInstallPrompt) {
        return;
      }
      deferredInstallPrompt.prompt();
      await deferredInstallPrompt.userChoice;
      deferredInstallPrompt = null;
      installButton.hidden = true;
      setNavOpen(false);
    });
  }

  if ("serviceWorker" in navigator) {
    window.addEventListener("load", function () {
      navigator.serviceWorker.register("/sw.js").catch(function () {
        return null;
      });
    });
  }

  window.addEventListener("load", function () {
    initializeCharts();
  });
})();
