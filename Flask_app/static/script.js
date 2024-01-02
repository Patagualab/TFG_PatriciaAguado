function downloadChart() {
        var chartContent = document.querySelector('.chart');

        html2canvas(chartContent).then(function(canvas) {
            var downloadLink = document.createElement('a');
            
            // Convertir el canvas a una URL de datos PNG
            var imgURL = canvas.toDataURL('image/png');
            downloadLink.href = imgURL;
            downloadLink.download = 'chart.png';

            document.body.appendChild(downloadLink);
            downloadLink.click();
            document.body.removeChild(downloadLink);
        });
}
