export HEADER_IMAGE="http://drive.images/helo.png"
export HEADER_TEXT="starting from string"
export MIN_SUPPRESSION_THRESHOLD=25
export DISABLE_STREAMLIT_HAMBURGER=true

 export CUSTOM_TOP_NAVIGATION_HTML=$(cat <<EOF
 <div>
 <a href="http://www.google.com" target="_blank">Go to google</a>
 <a href="http://www.bing.com" target="_blank">Go to bing</a>
 <a href="http://www.bing.com" target="_blank">Fill Survey</a>
 </div>
 )

start() {
    echo "current dir :"`pwd`
    echo "current env HEADER_IMAGE:$HEADER_IMAGE"
    echo "current env HEADER_TEXT:$HEADER_TEXT"
    echo "current env MIN_SUPPRESSION_THRESHOLD:$MIN_SUPPRESSION_THRESHOLD"
    echo "current env DISABLE_STREAMLIT_HAMBURGER:$DISABLE_STREAMLIT_HAMBURGER"
    streamlit run dart_app.py --server.headless true
}
start