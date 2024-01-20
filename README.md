
# Astronomer Starship
Starship is a utility to migrate Airflow metadata between two Airflow instances

<!--TOC-->

- [Astronomer Starship](#astronomer-starship)
- [WIP](#wip)
  - [Deps](#deps)
  - [Reference](#reference)
  - [Notes](#notes)

<!--TOC-->




# WIP
## Deps
https://github.com/apache/airflow/blob/0c10ddb3c6e9d8cbc1592d1e0bf5532c5ed4dfa9/airflow/www/package.json#L102C1-L147


## Reference
- https://tgul.dev/articles/using-vite-with-flask/
- https://medium.com/@pal.amittras/exploring-chakra-ui-part-2-setting-up-axios-react-query-and-react-router-dom-5d716a54875d#a3aa
- https://axios-http.com/docs/example
- https://formik.org/docs/tutorial
- https://github.com/apache/airflow/blob/main/airflow/www/static/js

## Notes
```html
<!-- ATTEMPT: Bootstrap. Inject static index.html built by Vite into the page after it loads -->
<!-- ERROR: <script> tag doesn't load after the page loads -->
<!--<main id="starship"></main>-->
<!--<script>-->
<!--    fetch("{{ url_for('starship.static', filename='index.html') }}")-->
<!--        .then(response => {-->
<!--            return response.text()-->
<!--        })-->
<!--        .then(data => {-->
<!--            document.querySelector("main#starship").outerHTML = data;-->
<!--        });-->
<!--</script>-->

<!-- ATTEMPT: iframe - works okay, but DOM is weird. Should be fine? -->
<iframe id="starship-main" src="{{ url_for('starship.static', filename='index.html') }}"></iframe>

<!-- ATTEMPT: iframe that injects itself into the page after loading? ERROR: some stuff didn't seem to load right -->
<!--       onload="this.insertAdjacentHTML('afterend', (this.contentDocument.body||this.contentDocument).innerHTML);this.remove()"-->
```
