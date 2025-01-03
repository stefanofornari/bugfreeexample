/*
 * <FUNAMBOLCOPYRIGHT>
 * Copyright (C) 2015 Funambol.
 * All Rights Reserved.  No use, copying or distribution of this
 * work may be made except in accodance with a valid license
 * agreement from Funambol.  This notice must be
 * included on all copies, modifications and derivatives of this
 * work.
 *
 * Funambol MAKES NO REPRESENTATIONS OR WARRANTIES ABOUT THE SUITABILITY
 * OF THE SOFTWARE, EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO
 * THE IMPLIED WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR
 * PURPOSE, OR NON-INFRINGEMENT. Funambol SHALL NOT BE LIABLE FOR ANY DAMAGES
 * SUFFERED BY LICENSEE AS A RESULT OF USING, MODIFYING OR DISTRIBUTING
 * THIS SOFTWARE OR ITS DERIVATIVES.
 * </FUNAMBOLCOPYRIGHT>
 */
package bugfree.example.angular;

import static org.assertj.core.api.BDDAssertions.then;
import org.junit.Test;
import org.mozilla.javascript.NativeArray;
import org.mozilla.javascript.NativeObject;
import ste.xtest.js.BugFreeEnvjs;
import ste.xtest.js.JSAssertions;
import ste.xtest.net.HttpClientStubber;
import ste.xtest.net.StubHttpClient;

/**
 *
 * @author ste
 */
public class BugFreeAngularController extends BugFreeEnvjs {
    public BugFreeAngularController() throws Exception {
        loadScript("src/main/webapp/phones/controllers.js");
        loadScript("src/test/angular/fake.js");
        exec("document.baseURI='http://localhost:8080/phones/'");
    }

    @Test
    public void load_index() throws Exception {
        givenURLStubs("phones1.json");

        exec("document.location='http://localhost:8080/phones/index.html';");

        then(exec("document.title;")).isEqualTo("Phone list");
    }

    @Test
    public void load_controller_and_data_from_url() throws Exception {
        givenURLStubs("phones1.json");

        exec("controller('PhoneListCtrl', {$scope: scope});");

        JSAssertions.then((NativeArray)exec("scope.phones;")).hasSize(3);
        thenPhoneIs(0, "Nexus S", "Fast just got faster with Nexus S.");
        thenPhoneIs(2, "MOTOROLA XOOM™", "The Next, Next Generation tablet.");
    }

    @Test
    public void content_is_dynamic() throws Exception {
        givenURLStubs("phones2.json");

        exec("controller('PhoneListCtrl', {$scope: scope});");

        JSAssertions.then((NativeArray)exec("scope.phones;")).hasSize(1);
        thenPhoneIs(0, "Huawei", "Best price/quality ratio");
    }

    @Test
    public void set_status_in_case_of_errors() throws Exception {
        //
        // connection error
        //
        givenURLStubsWithError(404); // not found

        exec("controller('PhoneListCtrl', {$scope: scope});");

        JSAssertions.then((NativeArray)exec("scope.phones;")).isEmpty();
        then(exec("scope.status;")).isEqualTo("error");

        //
        // in case of success status becomes "ok"
        //
        givenURLStubs("phones2.json");

        exec("controller('PhoneListCtrl', {$scope: scope});");

        then(exec("scope.status;")).isEqualTo("ok");

        //
        // server error
        //
        givenURLStubsWithError(500);

        exec("controller('PhoneListCtrl', {$scope: scope});");

        JSAssertions.then((NativeArray)exec("scope.phones;")).isEmpty();
        then(exec("scope.status;")).isEqualTo("error");
    }

    /**
     * The url to retrieve the phone list shall be relative to current document
     * location.
     *
     * @throws Exception
     */
    @Test
    public void dynamic_data_retrieving_url() throws Exception {
        debug(true);
        givenURLStubs("phones1.json");

        exec("controller('PhoneListCtrl', {$scope: scope});");

        thenPhoneIs(0, "Nexus S", "Fast just got faster with Nexus S.");
    }

    @Test
    public void list_of_phones1() throws Exception {
        givenURLStubs("phones1.json");

        exec("document.location='http://localhost:8080/phones/index.html';");
        exec("controller('PhoneListCtrl', {$scope: scope});");

        then(exec("$('ul li:first span').text()")).isEqualTo("Nexus S");
        then(exec("$('ul li:first p').text()")).isEqualTo("Fast just got faster with Nexus S.");
    }

    @Test
    public void list_of_phones2() throws Exception {
        givenURLStubs("server.com", "phones2.json");

        exec("document.location='http://server.com/phones/index.html';");
        exec("controller('PhoneListCtrl', {$scope: scope});");

        then(exec("$('ul li:first span').text()")).isEqualTo("Huawei");
        then(exec("$('ul li:first p').text()")).isEqualTo("Best price/quality ratio");
    }

    // --------------------------------------------------------- private methods

    private void givenURLStubs(final String server, final String file) throws Exception {
        final HttpClientStubber stubber = httpStubber();
        stubber.stubs().clear();
        stubber.withStub(
            "http://" + server +"/phones/index.html",
            new StubHttpClient.StubHttpResponse()
                .file("src/main/webapp/phones/index.html").contentType("text/html")
        );
        stubber.withStub(
            "http://" + server + "/phones/phones.json",
            new StubHttpClient.StubHttpResponse()
                .file("src/test/angular/" + file).contentType("application/json")
        );
    }

    private void givenURLStubs(final String file) throws Exception {
        givenURLStubs("localhost:8080", file);
    }

    private void givenURLStubsWithError(int status) throws Exception {
        final HttpClientStubber stubber = httpStubber();
        stubber.stubs().clear();
        stubber.withStub(
            "http://localhost:8080/phones/phones.json",
            new StubHttpClient.StubHttpResponse()
                .text("not found").statusCode(status)
        );
    }

    private void thenPhoneIs(int index, final String phone, final String snippet)
    throws Exception {
        NativeObject p = (NativeObject)exec("scope.phones[" + index + "];");
        then(p.get("name", null)).isEqualTo(phone);
        then(p.get("snippet", null)).isEqualTo(snippet);
    }
}
