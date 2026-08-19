FROM nginx:1.27-alpine

COPY ui/nginx.conf /etc/nginx/conf.d/default.conf
COPY ui/index.html /usr/share/nginx/html/index.html
COPY ui/app.js /usr/share/nginx/html/app.js
COPY ui/app.css /usr/share/nginx/html/app.css
