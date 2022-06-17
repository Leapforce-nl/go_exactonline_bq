package exactonline_bq

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"cloud.google.com/go/storage"

	errortools "github.com/leapforce-libraries/go_errortools"
	financial "github.com/leapforce-libraries/go_exactonline_new/financial"
	types "github.com/leapforce-libraries/go_types"
)

type OutstandingInvoicesOverview struct {
	OrganisationID_                    int64
	SoftwareClientLicenceID_           int64
	SoftwareClientLicenseGuid_         string
	Created_                           time.Time
	Modified_                          time.Time
	CurrencyCode                       string
	OutstandingPayableInvoiceAmount    float64
	OutstandingPayableInvoiceCount     float64
	OutstandingReceivableInvoiceAmount float64
	OutstandingReceivableInvoiceCount  float64
	OverduePayableInvoiceAmount        float64
	OverduePayableInvoiceCount         float64
	OverdueReceivableInvoiceAmount     float64
	OverdueReceivableInvoiceCount      float64
}

func getOutstandingInvoicesOverview(c *financial.OutstandingInvoicesOverview, organisationID int64, softwareClientLicenceID int64, softwareClientLicenseGuid string) OutstandingInvoicesOverview {
	t := time.Now()

	return OutstandingInvoicesOverview{
		organisationID,
		softwareClientLicenceID,
		softwareClientLicenseGuid,
		t, t,
		c.CurrencyCode,
		c.OutstandingPayableInvoiceAmount,
		c.OutstandingPayableInvoiceCount,
		c.OutstandingReceivableInvoiceAmount,
		c.OutstandingReceivableInvoiceCount,
		c.OverduePayableInvoiceAmount,
		c.OverduePayableInvoiceCount,
		c.OverdueReceivableInvoiceAmount,
		c.OverdueReceivableInvoiceCount,
	}
}

func (service *Service) WriteOutstandingInvoicesOverviews(bucketHandle *storage.BucketHandle, organisationID int64, softwareClientLicenceID int64, softwareClientLicenseGuid string, _ *time.Time) ([]*storage.ObjectHandle, int, interface{}, *errortools.Error) {
	if bucketHandle == nil {
		return nil, 0, nil, nil
	}

	objectHandles := []*storage.ObjectHandle{}
	var w *storage.Writer

	call := service.FinancialService().NewGetOutstandingInvoicesOverviewsCall()

	rowCount := 0
	batchRowCount := 0
	batchSize := 10000

	for {
		outstandingInvoicesOverviews, e := call.Do()
		if e != nil {
			return nil, 0, nil, e
		}

		if outstandingInvoicesOverviews == nil {
			break
		}

		if batchRowCount == 0 {
			guid := types.NewGuid()
			objectHandle := bucketHandle.Object((&guid).String())
			objectHandles = append(objectHandles, objectHandle)

			w = objectHandle.NewWriter(context.Background())
		}

		for _, tl := range *outstandingInvoicesOverviews {
			batchRowCount++

			b, err := json.Marshal(getOutstandingInvoicesOverview(&tl, organisationID, softwareClientLicenceID, softwareClientLicenseGuid))
			if err != nil {
				return nil, 0, nil, errortools.ErrorMessage(err)
			}

			// Write data
			_, err = w.Write(b)
			if err != nil {
				return nil, 0, nil, errortools.ErrorMessage(err)
			}

			// Write NewLine
			_, err = fmt.Fprintf(w, "\n")
			if err != nil {
				return nil, 0, nil, errortools.ErrorMessage(err)
			}
		}

		if batchRowCount > batchSize {
			// Close and flush data
			err := w.Close()
			if err != nil {
				return nil, 0, nil, errortools.ErrorMessage(err)
			}
			w = nil

			fmt.Printf("#OutstandingInvoicesOverviews flushed: %v\n", batchRowCount)

			rowCount += batchRowCount
			batchRowCount = 0
		}
	}

	if w != nil {
		// Close and flush data
		err := w.Close()
		if err != nil {
			return nil, 0, nil, errortools.ErrorMessage(err)
		}

		rowCount += batchRowCount
	}

	fmt.Printf("#OutstandingInvoicesOverviews: %v\n", rowCount)

	return objectHandles, rowCount, OutstandingInvoicesOverview{}, nil
}
