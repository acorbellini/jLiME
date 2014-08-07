$(document).ready(function() {
	$('[data-toggle=offcanvas]').click(function() {
		$('.row-offcanvas').toggleClass('active');
	});
	setPage([ {
		name : 'page',
		value : document.location.hash.substring(1)
	} ]);
});
function changedHash() {
	setPage([ {
		name : 'page',
		value : document.location.hash.substring(1)
	} ]);
}
window.onhashchange = changedHash;